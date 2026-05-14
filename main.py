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
LEVERAGE = int(os.getenv("LEVERAGE", "20"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "3.0")) / 100.0  # 3% of balance
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("BitgetBot-Adv")


class BitgetAdvancedBot:
    """
    TSM Advanced 2.0 - High Frequency Trend Following Bot.
    
    Strategy:
    - Trend: Price > EMA 50 > EMA 200 (Long) | Price < EMA 50 < EMA 200 (Short)
    - Momentum: RSI pullback (Long < 40, Short > 60)
    - Position Sizing: Risk 3% of total equity based on ATR Stop Loss
    - Take Profit: Partial closure at 2:1 and 4:1 RR
    """

    def __init__(self) -> None:
        config = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "password": API_PASSPHRASE,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if proxy_url:
            p_url = proxy_url if proxy_url.startswith("http") else f"http://{proxy_url}"
            config["proxies"] = {"http": p_url, "https": p_url}

        self.exchange = ccxt.bitget(config)
        self.is_running = True

    async def initialize(self) -> None:
        try:
            logger.info(f"Initializing bot for {SYMBOL} with {LEVERAGE}x leverage")
            try:
                await self.exchange.set_margin_mode("isolated", SYMBOL)
            except: pass
            await self.exchange.set_leverage(LEVERAGE, SYMBOL)
        except Exception as e:
            logger.error(f"Initialization error: {e}")
            raise

    async def fetch_data(self) -> pd.DataFrame:
        ohlcv = await self.exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=250)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["ema_50"] = ta.ema(df["close"], length=50)
        df["ema_200"] = ta.ema(df["close"], length=200)
        df["rsi_14"] = ta.rsi(df["close"], length=14)
        df["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        return df

    async def get_balance_usdt(self) -> float:
        """Fetches the available USDT balance in the futures account."""
        balance = await self.exchange.fetch_balance()
        # For Bitget swap, balance is often under 'USDT' in 'total' or 'free'
        return float(balance.get("USDT", {}).get("total", 0.0))

    async def calculate_risk_based_qty(self, entry_price: float, sl_price: float) -> Tuple[float, float]:
        """
        Calculates position size based on balance and risk percentage.
        Formula: Qty = (Equity * Risk%) / abs(Entry - SL)
        """
        equity = await self.get_balance_usdt()
        if equity <= 0:
            logger.warning("Equity is 0 or not found.")
            return 0.0, 0.0

        risk_amount = equity * RISK_PERCENT
        price_risk = abs(entry_price - sl_price)
        
        if price_risk == 0: return 0.0, 0.0
        
        raw_qty = risk_amount / price_risk
        
        # Check if we have enough margin
        required_margin = (raw_qty * entry_price) / LEVERAGE
        if required_margin > equity * 0.9:  # Cap at 90% of equity to avoid instant liquidation
            logger.warning("Risk-based qty exceeds safe margin. Capping at 90% equity.")
            raw_qty = (equity * 0.9 * LEVERAGE) / entry_price
            
        qty = float(self.exchange.amount_to_precision(SYMBOL, raw_qty))
        return qty, equity

    async def execute_trade(self, side: str, qty: float, sl: float, tp1: float, tp2: float) -> None:
        if DRY_RUN:
            logger.info(f"[DRY RUN] {side} {qty} {SYMBOL} | SL: {sl} | TP1: {tp1} | TP2: {tp2}")
            return

        try:
            # 1. Entry
            order = await self.exchange.create_market_order(SYMBOL, side, qty)
            logger.info(f"Entry {side} executed: {order['id']} | Qty: {qty}")

            # 2. Trigger Orders (Stop Loss & Partial Take Profits)
            exit_side = "sell" if side == "buy" else "buy"
            
            # SL for full position
            await self.exchange.create_order(SYMBOL, "market", exit_side, qty, None, {"stopPrice": sl, "reduceOnly": True})
            
            # Partial TP1 (50%)
            qty_half = float(self.exchange.amount_to_precision(SYMBOL, qty / 2))
            await self.exchange.create_order(SYMBOL, "market", exit_side, qty_half, None, {"stopPrice": tp1, "reduceOnly": True})
            
            # Partial TP2 (Rest)
            await self.exchange.create_order(SYMBOL, "market", exit_side, qty_half, None, {"stopPrice": tp2, "reduceOnly": True})
            
            logger.info(f"Protective orders set: SL {sl}, TP1 {tp1}, TP2 {tp2}")

        except Exception as e:
            logger.error(f"Execution failed: {e}")

    async def process_strategy(self) -> None:
        df = await self.fetch_data()
        if len(df) < 200: return

        last = df.iloc[-2]
        c, ema50, ema200, rsi, atr = last["close"], last["ema_50"], last["ema_200"], last["rsi_14"], last["atr_14"]

        # Check Position
        positions = await self.exchange.fetch_positions([SYMBOL])
        active_pos = next((p for p in positions if float(p["contracts"]) > 0), None)

        if active_pos:
            # Exit Logic (Trend Reversal)
            side = active_pos["side"]
            if (side == "long" and c < ema50) or (side == "short" and c > ema50):
                logger.info(f"Trend reversal exit for {side}")
                await self.exchange.cancel_all_orders(SYMBOL)
                await self.exchange.create_market_order(SYMBOL, "sell" if side == "long" else "buy", active_pos["contracts"], {"reduceOnly": True})
        else:
            # Entry Logic
            # LONG: Price > EMA 50 > EMA 200 AND RSI < 40
            if c > ema50 > ema200 and rsi < 40:
                sl = c - (2 * atr)
                qty, balance = await self.calculate_risk_based_qty(c, sl)
                if qty > 0:
                    tp1 = c + (2 * (c - sl)) # 2:1 RR
                    tp2 = c + (4 * (c - sl)) # 4:1 RR
                    logger.info(f"Signal: LONG | Bal: {balance:.2f} | Risk: {balance*RISK_PERCENT:.2f}")
                    await self.execute_trade("buy", qty, sl, tp1, tp2)

            # SHORT: Price < EMA 50 < EMA 200 AND RSI > 60
            elif c < ema50 < ema200 and rsi > 60:
                sl = c + (2 * atr)
                qty, balance = await self.calculate_risk_based_qty(c, sl)
                if qty > 0:
                    tp1 = c - (2 * (sl - c)) # 2:1 RR
                    tp2 = c - (4 * (sl - c)) # 4:1 RR
                    logger.info(f"Signal: SHORT | Bal: {balance:.2f} | Risk: {balance*RISK_PERCENT:.2f}")
                    await self.execute_trade("sell", qty, sl, tp1, tp2)

    async def main_loop(self) -> None:
        await self.initialize()
        while self.is_running:
            try:
                await self.process_strategy()
                # Dynamic sleep until next candle
                now = datetime.now()
                mins = int(TIMEFRAME[:-1])
                sleep_sec = (mins - (now.minute % mins)) * 60 - now.second + 5
                logger.info(f"Analysis complete. Sleeping {sleep_sec}s")
                await asyncio.sleep(max(sleep_sec, 10))
            except Exception as e:
                logger.error(f"Loop error: {e}")
                await asyncio.sleep(30)

if __name__ == "__main__":
    bot = BitgetAdvancedBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutdown.")
