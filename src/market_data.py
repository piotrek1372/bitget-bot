import asyncio
from collections import deque
from typing import Dict, Any, List, Optional
import pandas as pd
import ccxt.pro as ccxtpro
from .interfaces import IMarketData, IConfig, ILogger, IExchange

class MarketDataService(IMarketData):
    """
    Handles real-time market data via WebSockets and historical snapshots via REST.
    Uses a deque-based sliding window for high-performance candle management.
    """
    def __init__(self, config: IConfig, logger: ILogger, exchange: IExchange) -> None:
        self._config = config
        self._logger = logger
        self._exchange = exchange
        self._new_candle_event: Optional[asyncio.Event] = None
        
        # Buffer for multiple timeframes
        self._buffers: Dict[str, deque] = {
            self._config.BASE_TIMEFRAME: deque(maxlen=200)
        }
        for tf in getattr(self._config, "TREND_TIMEFRAMES", []):
            self._buffers[tf] = deque(maxlen=200)
            
        self._ws_client = ccxtpro.bitget({
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "password": getattr(config, "BITGET_API_PASSPHRASE", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        
        self._is_running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._sync_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        """Starts the synchronization process and the WebSocket listener."""
        self._is_running = True
        
        # 1. Start WS loop in background (starts buffering immediately)
        self._ws_task = asyncio.create_task(self._ws_loop())
        
        # 2. Perform "Warm-up": Fetch historical snapshots via REST
        await self._warm_up()
        
        # 3. Process the sync queue to bridge the gap
        await self._bridge_gap()
        
        self._logger.info("MarketDataService: Warm-up complete and synchronized.")

    async def _warm_up(self) -> None:
        """Fetches initial OHLCV data for all timeframes."""
        for tf in self._buffers.keys():
            self._logger.info(f"MarketDataService: Fetching REST snapshot for {tf}...")
            df = await self._exchange.fetch_ohlcv(self._config.symbol, tf, limit=200)
            
            # Convert DF rows to list of dicts for the deque
            for _, row in df.iterrows():
                self._buffers[tf].append(row.to_dict())

    async def _bridge_gap(self) -> None:
        """Processes buffered WS messages to ensure no data was lost during REST fetch."""
        while not self._sync_queue.empty():
            msg = await self._sync_queue.get()
            self._process_tick(msg)

    async def _ws_loop(self) -> None:
        """Main WebSocket listener loop."""
        while self._is_running:
            try:
                # CCXT Pro handles heartbeats and re-connections internally
                candles = await self._ws_client.watch_ohlcv(
                    self._config.symbol, 
                    timeframe=self._config.BASE_TIMEFRAME
                )
                
                for candle in candles:
                    # Format: [timestamp, open, high, low, close, volume]
                    data = {
                        "ts": pd.to_datetime(candle[0], unit="ms"),
                        "open": candle[1],
                        "high": candle[2],
                        "low": candle[3],
                        "close": candle[4],
                        "volume": candle[5]
                    }
                    
                    if self._sync_queue is not None:
                        await self._sync_queue.put(data)
                    else:
                        self._process_tick(data)
                        
            except Exception as e:
                self._logger.error(f"WebSocket Error: {e}")
                await asyncio.sleep(5)

    def _process_tick(self, tick: Dict[str, Any]) -> None:
        """Updates the internal buffer with a new tick/candle."""
        tf = self._config.BASE_TIMEFRAME
        buffer = self._buffers[tf]
        
        if not buffer:
            buffer.append(tick)
            return

        last_ts = buffer[-1]["ts"]
        
        if tick["ts"] > last_ts:
            # New candle started - Trigger event for orchestrator
            buffer.append(tick)
            if self._new_candle_event:
                self._new_candle_event.set()
        elif tick["ts"] == last_ts:
            # Update current candle
            buffer[-1] = tick

    def set_event_trigger(self, event: asyncio.Event) -> None:
        """Sets the event to be triggered when a new candle closes."""
        self._new_candle_event = event

    def get_latest_data(self) -> Dict[str, pd.DataFrame]:
        """Returns the current market state as DataFrames."""
        return {
            tf: pd.DataFrame(list(buf)) 
            for tf, buf in self._buffers.items() 
            if len(buf) > 0
        }

    async def stop(self) -> None:
        """Stops the service and closes connections."""
        self._is_running = False
        if self._ws_task:
            self._ws_task.cancel()
        await self._ws_client.close()
