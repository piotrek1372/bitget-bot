from typing import Optional
from .interfaces import IRiskManager, IConfig, ILogger
from .models import MarketSignal, SignalSide

class RiskManager(IRiskManager):
    """
    Handles position sizing, drawdown circuit breakers, and liquidation safety.
    The 'Fortress' gatekeeper of the system.
    """
    def __init__(self, config: IConfig, logger: ILogger) -> None:
        self._config = config
        self._logger = logger
        self._daily_start_balance: Optional[float] = None
        
        # Safe default for Maintenance Margin Rate (Bitget SUI is usually ~0.6%)
        self._maint_margin_rate = 0.006

    def validate_execution(self, signal: MarketSignal, balance: float) -> Optional[float]:
        """
        Calculates position size and validates safety parameters.
        
        Returns:
            Optional[float]: The allowed quantity in contracts, or None if rejected.
        """
        if signal.side == SignalSide.NONE:
            return None

        # 1. Initialize daily balance for drawdown tracking
        if self._daily_start_balance is None:
            self._daily_start_balance = balance

        # 2. Check Daily Drawdown Circuit Breaker
        if not self.check_circuit_breakers(balance):
            return None

        # 3. Calculate Liquidation Price Safety
        liq_price = self._calculate_liquidation_price(
            signal.entry_price, 
            self._config.LEVERAGE, 
            signal.side
        )
        
        # Verify SL is hit BEFORE Liquidation
        if signal.side == SignalSide.LONG:
            if signal.stop_loss <= liq_price:
                self._logger.warning(f"RISK REJECT: SL ({signal.stop_loss}) below Liq ({liq_price:.4f})")
                return None
        else:
            if signal.stop_loss >= liq_price:
                self._logger.warning(f"RISK REJECT: SL ({signal.stop_loss}) above Liq ({liq_price:.4f})")
                return None

        # 4. Position Sizing (Fixed Fractional)
        # Risk amount in dollars = balance * risk_percent
        risk_usd = balance * (self._config.RISK_PERCENT / 100.0)
        sl_dist = abs(signal.entry_price - signal.stop_loss)
        
        if sl_dist == 0:
            return None
            
        # Qty = Risk / Distance
        qty = risk_usd / sl_dist
        
        # 5. Leverage Constraint (Cannot exceed max buying power)
        max_qty = (balance * self._config.LEVERAGE * 0.95) / signal.entry_price
        final_qty = min(qty, max_qty)
        
        self._logger.info(
            f"Risk Check PASSED | Size: {final_qty:.2f} | "
            f"Liq: {liq_price:.4f} | SL: {signal.stop_loss:.4f}"
        )
        
        return final_qty

    def check_circuit_breakers(self, current_equity: float) -> bool:
        """Checks if the bot should stop trading due to losses."""
        if self._daily_start_balance is None:
            return True
            
        drawdown = (self._daily_start_balance - current_equity) / self._daily_start_balance
        if drawdown >= self._config.MAX_DAILY_DRAWDOWN:
            self._logger.critical(f"CIRCUIT BREAKER: Daily drawdown {drawdown*100:.2f}% exceeded limit.")
            return False
        return True

    def _calculate_liquidation_price(self, entry: float, leverage: int, side: SignalSide) -> float:
        """
        Estimates the liquidation price for isolated margin.
        Formula: Entry * (1 - 1/Lev + MMR) for Long.
        """
        if side == SignalSide.LONG:
            return entry * (1 - (1 / leverage) + self._maint_margin_rate)
        else:
            return entry * (1 + (1 / leverage) - self._maint_margin_rate)
