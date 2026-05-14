import asyncio
from typing import Optional, Dict, Any
from .interfaces import IExecutionManager, IExchange, ILogger, IStateManager
from .models import MarketSignal, OrderResult, SignalSide

class ExecutionManager(IExecutionManager):
    """
    Translates strategy signals into exchange orders with precision handling.
    Implements optimistic locking to prevent double-spending.
    """
    def __init__(
        self, 
        exchange: IExchange, 
        logger: ILogger, 
        state: IStateManager
    ) -> None:
        self._exchange = exchange
        self._logger = logger
        self._state = state

    async def execute_signal(self, signal: MarketSignal, quantity: float) -> Optional[OrderResult]:
        """
        Formats and executes a signal as a market order with SL/TP plans.
        """
        if signal.side == SignalSide.NONE or quantity <= 0:
            return None

        # 1. Apply Optimistic Lock
        self._state.set_pending_lock(True)
        
        try:
            # 2. Prepare Order Parameters (Bitget V2 Mix Order Plan)
            # We use CCXT's precision methods (internally fetched from exchange.load_markets)
            symbol = self._exchange.symbol
            
            # Formatting SL/TP with conservative rounding (handled by CCXT or manual truncation)
            params = {
                "presetStopLossPrice": self._format_price(symbol, signal.stop_loss),
                "presetTakeProfitPrice": self._format_price(symbol, signal.take_profit),
                "stopLossType": "market",
                "takeProfitType": "market"
            }

            side = "buy" if signal.side == SignalSide.LONG else "sell"
            
            self._logger.info(
                f"Executing {signal.side.value.upper()} | Qty: {quantity:.4f} | "
                f"SL: {params['presetStopLossPrice']} | TP: {params['presetTakeProfitPrice']}"
            )

            # 3. Submit Order
            order = await self._exchange.create_market_order(
                symbol, side, quantity, params
            )
            
            # 4. Start Watchdog for reconciliation (State management)
            asyncio.create_task(self._state.watchdog_reconciliation(order.order_id))
            
            return order

        except Exception as e:
            self._logger.error(f"Execution Error: {e}")
            self._state.set_pending_lock(False)
            return None

    def _format_price(self, symbol: str, price: float) -> str:
        """
        Wraps CCXT price formatting.
        In a real implementation, we would access the exchange's market metadata.
        """
        # Note: This assumes self._exchange (BitgetExchange) has loaded markets.
        # For this implementation, we proxy through the exchange instance.
        return self._exchange.price_to_precision(symbol, price)

    def _format_amount(self, symbol: str, amount: float) -> str:
        """Wraps CCXT amount formatting."""
        return self._exchange.amount_to_precision(symbol, amount)
