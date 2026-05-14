import asyncio
import signal
from typing import Optional
from .interfaces import (
    IConfig, ILogger, IExchange, IMarketData, 
    IStrategy, IRiskManager, IExecutionManager, IStateManager
)
from .models import SignalSide

class FortressBot:
    """
    Main Orchestrator for the Bitget trading system.
    Coordinates all modules using an event-driven loop triggered by new candles.
    """
    def __init__(
        self,
        config: IConfig,
        logger: ILogger,
        exchange: IExchange,
        market_data: IMarketData,
        strategy: IStrategy,
        risk: IRiskManager,
        execution: IExecutionManager,
        state: IStateManager
    ) -> None:
        self._config = config
        self._logger = logger
        self._exchange = exchange
        self._market_data = market_data
        self._strategy = strategy
        self._risk = risk
        self._execution = execution
        self._state = state
        
        self._is_running: bool = False
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._new_candle_event: asyncio.Event = asyncio.Event()

    async def run(self) -> None:
        """
        Starts the bot's execution loop and manages graceful shutdown.
        """
        self._logger.info("Initializing Fortress Engine (Finał)...")
        self._is_running = True
        
        # Link the event trigger to MarketDataService
        self._market_data.set_event_trigger(self._new_candle_event)
        
        try:
            # 1. Start all services (WS listeners)
            await self._market_data.start()
            await self._state.start()
            
            self._logger.info("Engine Online | Waiting for new candle event...")
            
            # 2. Event-Driven Loop
            while not self._shutdown_event.is_set():
                try:
                    # Wait for a new candle close or timeout for health check
                    await asyncio.wait_for(self._new_candle_event.wait(), timeout=60.0)
                    self._new_candle_event.clear()
                    
                    # Core synchronization check
                    if self._state.is_locked:
                        self._logger.warning("Cycle Skipped: State Manager is currently LOCKED.")
                        continue
                        
                    await self._process_cycle()
                    
                except asyncio.TimeoutError:
                    # Periodic heartbeat/health check
                    self._logger.info("Heartbeat: Waiting for market activity...")
                except Exception as e:
                    self._logger.error(f"Loop Cycle Error: {e}")
                    await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            self._logger.info("Bot execution cancelled.")
        except Exception as e:
            self._logger.critical(f"Fatal Engine Error: {e}")
        finally:
            await self.shutdown()

    async def _process_cycle(self) -> None:
        """
        Executes a single decision cycle when a new candle arrives.
        """
        # 1. Skip if already in position
        if self._state.current_position is not None:
            return

        # 2. Get latest MTF data
        market_data = self._market_data.get_latest_data()
        
        # 3. Generate Signal (Pure Logic)
        signal = self._strategy.calculate_signal(market_data)
        if signal.side == SignalSide.NONE:
            return

        # 4. Risk Validation (Capital Guard)
        balance = await self._exchange.fetch_balance()
        quantity = self._risk.validate_execution(signal, balance)
        
        if quantity and quantity > 0:
            # 5. Execution (Market Order + SL/TP)
            # Optimistic lock and Watchdog are handled inside ExecutionManager
            await self._execution.execute_signal(signal, quantity)
        else:
            self._logger.info(f"Signal {signal.side.value.upper()} REJECTED by Risk Manager.")

    async def shutdown(self) -> None:
        """
        Gracefully stops all background tasks and connections.
        """
        if not self._is_running:
            return
            
        self._logger.info("Initiating Graceful Shutdown...")
        self._is_running = False
        self._shutdown_event.set()
        
        # Stop all WS streams
        await self._market_data.stop()
        await self._state.stop()
        
        # Close REST client
        if hasattr(self._exchange, "close"):
            await self._exchange.close()
        
        self._logger.info("Fortress Engine Offline.")

def setup_signal_handlers(bot: FortressBot, loop: asyncio.AbstractEventLoop) -> None:
    """Configures handlers for SIGINT and SIGTERM."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(bot.shutdown()))
        except NotImplementedError:
            # Signal handlers not supported on some systems (like Windows during dev)
            pass
