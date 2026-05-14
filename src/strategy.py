import pandas as pd
import pandas_ta as ta
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
from .interfaces import IStrategy, ILogger, IConfig
from .models import MarketSignal, SignalSide

class StrategyEngine(IStrategy):
    """
    Pure logic engine for trend-following with liquidity sweep filters.
    Stateless: f(DataFrames) -> MarketSignal.
    """
    def __init__(self, config: IConfig, logger: ILogger) -> None:
        self._config = config
        self._logger = logger

    def calculate_signal(self, data: Dict[str, pd.DataFrame]) -> MarketSignal:
        """
        Analyzes MTF data to generate a trading signal.
        
        Args:
            data: Dictionary mapping timeframes to DataFrames with OHLCV data.
            
        Returns:
            MarketSignal: A signal object (side=NONE if no trade).
        """
        base_tf = self._config.BASE_TIMEFRAME
        if base_tf not in data or data[base_tf].empty:
            return self._empty_signal()

        df_base = self._apply_indicators(data[base_tf])
        last = df_base.iloc[-1]
        prev = df_base.iloc[-2]

        # 1. MTF Trend Filter (using 1h timeframe)
        trend_side = self._get_mtf_trend(data)
        if trend_side == SignalSide.NONE:
            return self._empty_signal()

        # 2. VWAP Guard (Safety check)
        if pd.isna(last["vwap"]):
            return self._empty_signal()
        
        if trend_side == SignalSide.LONG and last["close"] < last["vwap"]:
            return self._empty_signal()
        if trend_side == SignalSide.SHORT and last["close"] > last["vwap"]:
            return self._empty_signal()

        # 3. Trigger & Liquidity Filter
        side = SignalSide.NONE
        if trend_side == SignalSide.LONG:
            if prev["stoch_k"] < 20 and last["stoch_k"] > last["stoch_d"]:
                if self._is_liquidity_sweep(df_base, SignalSide.LONG):
                    side = SignalSide.LONG
        elif trend_side == SignalSide.SHORT:
            if prev["stoch_k"] > 80 and last["stoch_k"] < last["stoch_d"]:
                if self._is_liquidity_sweep(df_base, SignalSide.SHORT):
                    side = SignalSide.SHORT

        if side == SignalSide.NONE:
            return self._empty_signal()

        # 4. Calculate Risk Parameters (SL/TP)
        entry = last["close"]
        atr = last["atr"]
        sl_dist = 2.5 * atr
        
        sl = entry - sl_dist if side == SignalSide.LONG else entry + sl_dist
        tp = entry + (3.5 * sl_dist) if side == SignalSide.LONG else entry - (3.5 * sl_dist)

        return MarketSignal(
            side=side,
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            atr=atr,
            timestamp=datetime.now(),
            metadata={"reason": "MTF Trend + Stoch Cross + Liquidity Sweep"}
        )

    def _apply_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates indicators on the DataFrame."""
        df = df.copy()
        df["ema_50"] = ta.ema(df["close"], length=50)
        df["ema_200"] = ta.ema(df["close"], length=200)
        df["rsi"] = ta.rsi(df["close"], length=14)
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        df["vwap"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        
        stoch_rsi = ta.stochrsi(df["close"], length=14, rsi_length=14, k=3, d=3)
        df["stoch_k"] = stoch_rsi.iloc[:, 0] # STOCHRSIk
        df["stoch_d"] = stoch_rsi.iloc[:, 1] # STOCHRSId
        return df

    def _get_mtf_trend(self, data: Dict[str, pd.DataFrame]) -> SignalSide:
        """Determines the trend on a higher timeframe (1h)."""
        tf_1h = "1h"
        if tf_1h not in data or len(data[tf_1h]) < 200:
            return SignalSide.NONE
            
        df_1h = self._apply_indicators(data[tf_1h])
        last_1h = df_1h.iloc[-1]
        
        if last_1h["ema_50"] > last_1h["ema_200"]:
            return SignalSide.LONG
        if last_1h["ema_50"] < last_1h["ema_200"]:
            return SignalSide.SHORT
        return SignalSide.NONE

    def _is_liquidity_sweep(self, df: pd.DataFrame, side: SignalSide) -> bool:
        """Checks for support/resistance wicks in the last 3 closed candles."""
        last_3 = df.iloc[-4:-1]
        for _, row in last_3.iterrows():
            body = abs(row["open"] - row["close"])
            if side == SignalSide.LONG:
                lower_wick = min(row["open"], row["close"]) - row["low"]
                if lower_wick < body: return False
            else:
                upper_wick = row["high"] - max(row["open"], row["close"])
                if upper_wick < body: return False
        return True

    def _empty_signal(self) -> MarketSignal:
        return MarketSignal(
            side=SignalSide.NONE,
            entry_price=0.0,
            stop_loss=0.0,
            take_profit=0.0,
            atr=0.0,
            timestamp=datetime.now(),
            metadata={}
        )
