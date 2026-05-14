import asyncio
from typing import Optional, List, Dict, Any
import ccxt.pro as ccxtpro
from .interfaces import IStateManager, IConfig, ILogger, IExchange
from .models import Position, SignalSide

class StateManager(IStateManager):
    """
    Manages the bot's internal state (positions, balance).
    Uses WebSocket for real-time updates and REST for reconciliation.
    """
    def __init__(self, config: IConfig, logger: ILogger, exchange: IExchange) -> None:
        self._config = config
        self._logger = logger
        self._exchange = exchange
        
        self._current_pos: Optional[Position] = None
        self._is_pending_lock: bool = False
        self._is_running: bool = False
        self._ws_task: Optional[asyncio.Task] = None
        
        self._ws_client = ccxtpro.bitget({
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "password": getattr(config, "BITGET_API_PASSPHRASE", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })

    @property
    def is_locked(self) -> bool:
        return self._is_pending_lock

    @property
    def current_position(self) -> Optional[Position]:
        return self._current_pos

    def set_pending_lock(self, locked: bool) -> None:
        """Sets the optimistic lock to prevent new orders during transition."""
        self._is_pending_lock = locked
        state = "LOCKED" if locked else "UNLOCKED"
        self._logger.info(f"State Manager: Optimistic lock {state}")

    async def start(self) -> None:
        """Initializes state via REST and starts WS listener."""
        self._is_running = True
        
        # 1. Initial State Sync (REST)
        await self._reconcile_state()
        
        # 2. Start WS Loop
        self._ws_task = asyncio.create_task(self._ws_loop())

    async def _reconcile_state(self) -> None:
        """Force-syncs state with the exchange via REST."""
        try:
            positions = await self._exchange.fetch_positions()
            active = [p for p in positions if float(p["contracts"]) > 0]
            
            if active:
                p = active[0]
                self._current_pos = Position(
                    symbol=p["symbol"],
                    side=SignalSide.LONG if p["side"] == "long" else SignalSide.SHORT,
                    contracts=float(p["contracts"]),
                    entry_price=float(p["entryPrice"]),
                    unrealized_pnl=float(p["unrealizedPnl"]),
                    leverage=int(p["leverage"]),
                    margin_mode=p["marginMode"]
                )
            else:
                self._current_pos = None
                
            self.set_pending_lock(False)
            status = self._current_pos.side.value if self._current_pos else "NONE"
            self._logger.info(f"State Sync Complete | Position: {status}")
            
        except Exception as e:
            self._logger.error(f"State Reconciliation Error: {e}")

    async def watchdog_reconciliation(self, order_id: str, timeout: float = 3.0) -> None:
        """
        Fallback mechanism: if WS doesn't update state within timeout,
        force a REST reconciliation.
        """
        await asyncio.sleep(timeout)
        if self._is_pending_lock:
            self._logger.warning(f"Watchdog Triggered: WS update for order {order_id} timed out. Reconciling via REST...")
            await self._reconcile_state()

    async def listen_user_data(self) -> None:
        """Listens for User Data updates (Positions/Orders)."""
        while self._is_running:
            try:
                # CCXT Pro watch_positions
                positions = await self._ws_client.watch_positions([self._config.symbol])
                
                # Update local state immediately
                active = [p for p in positions if float(p["contracts"]) > 0]
                if active:
                    p = active[0]
                    self._current_pos = Position(
                        symbol=p["symbol"],
                        side=SignalSide.LONG if p["side"] == "long" else SignalSide.SHORT,
                        contracts=float(p["contracts"]),
                        entry_price=float(p["entryPrice"]),
                        unrealized_pnl=float(p["unrealizedPnl"]),
                        leverage=int(p["leverage"]),
                        margin_mode=p["marginMode"]
                    )
                else:
                    self._current_pos = None
                
                # Release lock if WS received valid position update
                self.set_pending_lock(False)
                
            except Exception as e:
                self._logger.error(f"User Data Stream Error: {e}")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._is_running = False
        if self._ws_task:
            self._ws_task.cancel()
        await self._ws_client.close()
