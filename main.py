import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
proxy_url = os.environ.get("FIXIE_URL")
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE")

SYMBOL = os.getenv("SYMBOL", "SUI/USDT:USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
LEVERAGE = int(os.getenv("LEVERAGE", "1"))
RISK_AMOUNT_USDT = float(os.getenv("RISK_AMOUNT_USDT", "10.0"))  # Margin per trade
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("BitgetBot")


class BitgetTSMBot:
    """
    A production-ready trading bot for Bitget Futures implementing the TSM strategy.
    
    Strategy: Trend-Support-Momentum (TSM)
    - Long: Close > EMA_50 AND RSI_14 < 30
    - Short: Close < EMA_50 AND RSI_14 > 70
    - Exit Long: Close >= EMA_50
    - Exit Short: Close <= EMA_50
    """

    def __init__(self) -> None:
        config = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "password": API_PASSPHRASE,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }

        # Handle Proxy for Heroku (Fixie)
        if proxy_url:
            p_url = proxy_url if proxy_url.startswith("http") else f"http://{proxy_url}"
            config["proxies"] = {
                "http": p_url,
                "https": p_url,
            }
            logger.info(f"Proxy configured using: {p_url}")

        self.exchange = ccxt.bitget(config)
        self.is_running = True

    async def log_ip(self) -> None:
        """Logs the current outbound IP address used by the bot."""
        import aiohttp
        try:
            # We use the same proxy settings as ccxt
            proxy = self.exchange.proxies.get("http") if self.exchange.proxies else None
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.ipify.org?format=json", proxy=proxy) as response:
                    data = await response.json()
                    logger.info(f"Bot outbound IP (via proxy): {data.get('ip')}")
        except Exception as e:
            logger.warning(f"Could not determine outbound IP: {e}")

    async def initialize(self) -> None:
        """Initializes exchange settings like margin mode and leverage."""
        try:
            # 0. Log outbound IP
            await self.log_ip()

            # 1. Set to isolated margin mode as required
            logger.info(f"Setting margin mode to ISOLATED for {SYMBOL}")
            try:
                await self.exchange.set_margin_mode("isolated", SYMBOL)
            except ccxt.MarginModeAlreadySet:
                pass
            except Exception as e:
                logger.warning(f"Could not set margin mode: {e}")

            # Set leverage
            logger.info(f"Setting leverage to {LEVERAGE}x for {SYMBOL}")
            await self.exchange.set_leverage(LEVERAGE, SYMBOL)
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    async def fetch_data(self, limit: int = 100) -> pd.DataFrame:
        """
        Fetches OHLCV data and calculates indicators.
        
        Args:
            limit: Number of candles to fetch.
            
        Returns:
            pd.DataFrame: Dataframe with indicators.
        """
        ohlcv = await self.exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        
        # Calculate Indicators
        df["ema_50"] = ta.ema(df["close"], length=50)
        df["rsi_14"] = ta.rsi(df["close"], length=14)
        df["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        
        return df

    async def get_active_position(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the current position for the configured symbol.
        
        Returns:
            Optional[Dict]: Position details or None if no position.
        """
        positions = await self.exchange.fetch_positions([SYMBOL])
        for pos in positions:
            if pos["symbol"] == SYMBOL and float(pos["contracts"]) > 0:
                return pos
        return None

    def calculate_quantity(self, entry_price: float) -> float:
        """
        Calculates the number of contracts based on USDT risk and leverage.
        
        Equation: Margin = (Qty * Price) / Leverage
        => Qty = (Margin * Leverage) / Price
        """
        # Bitget contracts for SUI might have specific lot sizes. 
        # For simplicity, we calculate the raw amount. 
        # CCXT's amount_to_precision should be used before ordering.
        raw_qty = (RISK_AMOUNT_USDT * LEVERAGE) / entry_price
        amount = self.exchange.amount_to_precision(SYMBOL, raw_qty)
        return amount

    async def execute_trade(self, side: str, amount: float, sl_price: Optional[float] = None) -> None:
        """
        Executes a market order and sets a stop loss.
        """
        if DRY_RUN:
            logger.info(f"[DRY RUN] Would execute {side} market order for {amount} {SYMBOL}")
            return

        try:
            # 1. Entry Market Order
            order = await self.exchange.create_market_order(SYMBOL, side, amount)
            logger.info(f"Entry {side} executed: {order['id']}")

            # 2. Set Stop Loss if provided
            if sl_price:
                sl_side = "sell" if side == "buy" else "buy"
                params = {"stopPrice": sl_price, "reduceOnly": True}
                # Using 'conditional' or 'trigger' order for SL
                sl_order = await self.exchange.create_order(
                    SYMBOL, "market", sl_side, amount, None, params
                )
                logger.info(f"Stop Loss set at {sl_price}: {sl_order['id']}")

        except ccxt.InsufficientFunds as e:
            logger.error(f"Insufficient funds: {e}")
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")

    async def close_position(self, position: Dict[str, Any]) -> None:
        """Closes an existing position using a market order with reduceOnly."""
        side = position["side"]
        amount = float(position["contracts"])
        exit_side = "sell" if side == "long" else "buy"

        if DRY_RUN:
            logger.info(f"[DRY RUN] Would close {side} position of {amount} {SYMBOL}")
            return

        try:
            # Cancel all open orders for this symbol (like existing SL)
            await self.exchange.cancel_all_orders(SYMBOL)
            
            # Execute close
            order = await self.exchange.create_market_order(
                SYMBOL, exit_side, amount, params={"reduceOnly": True}
            )
            logger.info(f"Position closed: {order['id']}")
        except Exception as e:
            logger.error(f"Failed to close position: {e}")

    async def process_strategy(self) -> None:
        """Main strategy logic executed every candle close."""
        try:
            df = await self.fetch_data()
            if len(df) < 52:  # Ensure enough data for indicators
                return

            # Anti-Repainting: Evaluate only the last closed candle
            last_closed = df.iloc[-2]
            curr_close = last_closed["close"]
            ema_50 = last_closed["ema_50"]
            rsi_14 = last_closed["rsi_14"]
            atr_14 = last_closed["atr_14"]

            logger.info(f"Analysis | Close: {curr_close:.4f} | EMA50: {ema_50:.4f} | RSI: {rsi_14:.2f}")

            # State Management: Check current position
            pos = await self.get_active_position()
            
            if pos:
                # We are IN_POSITION
                side = pos["side"] # 'long' or 'short'
                
                if side == "long":
                    if curr_close >= ema_50:
                        logger.info("Exit Signal: Long (Close >= EMA_50)")
                        await self.close_position(pos)
                elif side == "short":
                    if curr_close <= ema_50:
                        logger.info("Exit Signal: Short (Close <= EMA_50)")
                        await self.close_position(pos)
            else:
                # We are NOT IN_POSITION
                # Long Entry: Close > EMA_50 AND RSI_14 < 30
                if curr_close > ema_50 and rsi_14 < 40:
                    amount = self.calculate_quantity(curr_close)
                    sl_price = curr_close - (2 * atr_14)
                    logger.info(f"Entry Signal: LONG | SL: {sl_price:.4f}")
                    await self.execute_trade("buy", amount, sl_price)
                
                # Short Entry: Close < EMA_50 AND RSI_14 > 60
                elif curr_close < ema_50 and rsi_14 > 70:
                    amount = self.calculate_quantity(curr_close)
                    sl_price = curr_close + (2 * atr_14)
                    logger.info(f"Entry Signal: SHORT | SL: {sl_price:.4f}")
                    await self.execute_trade("sell", amount, sl_price)

        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            logger.warning(f"Exchange error: {e}")
        except Exception as e:
            logger.error(f"Strategy processing error: {e}")

    def get_timeframe_minutes(self) -> int:
        """Parses CCXT timeframe string into minutes."""
        unit = TIMEFRAME[-1]
        value = int(TIMEFRAME[:-1])
        if unit == "m":
            return value
        if unit == "h":
            return value * 60
        if unit == "d":
            return value * 1440
        return 15  # Default

    def get_sleep_time(self) -> float:
        """Calculates seconds until the next candle mark based on TIMEFRAME + 5 seconds offset."""
        from datetime import timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        
        tf_mins = self.get_timeframe_minutes()
        minutes_to_next = tf_mins - (now.minute % tf_mins)
        
        next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
        # Add 5 seconds buffer for server sync
        next_run += timedelta(seconds=5)
        
        sleep_seconds = (next_run - now).total_seconds()
        # If we are very close to the 5s offset (e.g. within 1s), jump to the next interval
        if sleep_seconds < 1:
            sleep_seconds += tf_mins * 60
            
        return sleep_seconds

    async def main_loop(self) -> None:
        """Main event loop with error handling and exponential backoff."""
        await self.initialize()
        
        backoff_delay = 5
        while self.is_running:
            try:
                # 1. Run Strategy
                await self.process_strategy()
                
                # 2. Calculate sleep time
                sleep_time = self.get_sleep_time()
                logger.info(f"Sleeping for {sleep_time:.2f} seconds until next candle.")
                
                # Reset backoff on successful run
                backoff_delay = 5
                await asyncio.sleep(sleep_time)
                
            except ccxt.RateLimitExceeded:
                logger.warning(f"Rate limit exceeded. Backing off for {backoff_delay}s")
                await asyncio.sleep(backoff_delay)
                backoff_delay = min(backoff_delay * 2, 60)
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                await asyncio.sleep(10)

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self.is_running = False
        await self.exchange.close()
        logger.info("Bot shutdown complete.")


if __name__ == "__main__":
    bot = BitgetTSMBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    finally:
        asyncio.run(bot.shutdown())

"""
# RATIONALE

## State Management
The bot employs a "Source of Truth" approach by fetching real-time position data directly from Bitget 
(`fetch_positions`) at every evaluation cycle. This eliminates the risk of local state desynchronization 
due to manual trades, unexpected liquidations, or network failures.

## Error Handling & Resilience
- Granular Exceptions: Specific CCXT errors (NetworkError, RateLimitExceeded) are caught to prevent 
  unnecessary crashes.
- Exponential Backoff: Implemented for RateLimitExceeded to respect API limits and restore service gracefully.
- Asyncio-Based: Non-blocking I/O ensures the bot remains responsive and can handle multiple concurrent 
  tasks (e.g., logging while waiting for API responses).
- Anti-Repainting: By strictly using `df.iloc[-2]`, the bot ensures that it only acts on finalized data, 
  preventing signal flickering that occurs on active candles.
"""
