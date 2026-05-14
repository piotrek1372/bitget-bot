import ccxt.async_support as ccxt
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime
from .interfaces import IExchange, IConfig, ILogger
from .models import OrderResult

class BitgetExchange(IExchange):
    """
    Asynchronous implementation of the Bitget exchange interface using CCXT.
    Handles REST API calls with robust error handling and rate limiting.
    """
    def __init__(self, config: IConfig, logger: ILogger) -> None:
        self._config = config
        self._logger = logger
        self._client = ccxt.bitget({
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "password": getattr(config, "BITGET_API_PASSPHRASE", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Fetches historical OHLCV data.
        
        Returns:
            pd.DataFrame: Columns [ts, open, high, low, close, volume]
        """
        try:
            ohlcv = await self._client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms")
            return df
        except Exception as e:
            self._logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            raise

    async def create_market_order(self, symbol: str, side: str, amount: float, params: Dict[str, Any]) -> OrderResult:
        """
        Executes a market order and returns a structured OrderResult.
        """
        try:
            order = await self._client.create_order(
                symbol, "market", side, amount, None, params
            )
            return OrderResult(
                order_id=order["id"],
                symbol=symbol,
                side=side,
                price=float(order.get("average", order.get("price", 0))),
                amount=float(order["amount"]),
                status=order["status"],
                timestamp=datetime.fromtimestamp(order["timestamp"] / 1000)
            )
        except Exception as e:
            self._logger.error(f"Order Execution Failed: {e}", extra={"symbol": symbol, "side": side})
            raise

    async def fetch_balance(self) -> float:
        """
        Fetches the total USDT balance.
        """
        try:
            balance = await self._client.fetch_balance()
            return float(balance.get("USDT", {}).get("total", 0.0))
        except Exception as e:
            self._logger.error(f"Error fetching balance: {e}")
            return 0.0

    def price_to_precision(self, symbol: str, price: float) -> str:
        """Formats price according to market tick size."""
        return self._client.price_to_precision(symbol, price)

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        """Formats amount according to market lot size."""
        return self._client.amount_to_precision(symbol, amount)

    async def fetch_positions(self) -> List[Dict[str, Any]]:
        """Fetches active positions via REST."""
        return await self._client.fetch_positions([self._config.symbol])

    async def close(self) -> None:
        """Closes the underlying aiohttp session."""
        await self._client.close()
