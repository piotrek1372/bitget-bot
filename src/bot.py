# src/bot.py
import asyncio
import logging
from typing import NoReturn

from .interfaces import (
    IConfig, ILogger, IExchange, IMarketData, 
    IStrategy, IRiskManager, IExecutionManager, IStateManager
)

class FortressBot:
    """Główny koordynator cyklu życia bota tradingowego."""

    def __init__(
        self,
        config: IConfig,
        logger: ILogger,
        exchange: IExchange,
        market_data: IMarketData,
        strategy: IStrategy,
        risk_manager: IRiskManager,
        execution_manager: IExecutionManager,
        state_manager: IStateManager
    ) -> None:
        self._config = config
        self._logger = logging.getLogger("FortressBot")
        self._exchange = exchange
        self._market_data = market_data
        self._strategy = strategy
        self._risk = risk_manager
        self._execution = execution_manager
        self._state = state_manager
        
        self._is_running = False
        # Event synchronizujący pętle WebSocket
        self._new_candle_event = asyncio.Event()
        self._market_data.set_candle_event(self._new_candle_event)

    async def run(self) -> NoReturn:
        """Główna pętla asynchroniczna systemu."""
        self._logger.info("Inicjalizacja FortressBot. Uruchamianie zadań w tle...")
        self._is_running = True

        try:
            # Uruchomienie niezależnych zadań nasłuchujących WebSocket
            market_task = asyncio.create_task(self._market_data.listen_websocket())
            state_task = asyncio.create_task(self._state.listen_user_data())

            while self._is_running:
                # Oczekiwanie na zamknięcie świecy przez MarketDataService
                await self._new_candle_event.wait()
                self._new_candle_event.clear()

                if self._state.is_locked():
                    self._logger.warning("Pominięto cykl: System oczekuje na rekoncyliację zleceń.")
                    continue

                await self._execution_cycle()

        except asyncio.CancelledError:
            self._logger.info("Pętla główna anulowana. Trwa zamykanie...")
        finally:
            await self._cleanup()

    async def _execution_cycle(self) -> None:
        """Pojedynczy cykl decyzyjny wyzwalany nową świecą."""
        try:
            df_dict = self._market_data.get_dataframes()
            signal = self._strategy.calculate_signal(df_dict)
            
            if not signal:
                return

            balance = await self._state.get_available_balance()
            quantity = self._risk.validate_execution(signal, balance)

            if quantity:
                await self._execution.execute_signal(signal, quantity)
                
        except Exception as e:
            self._logger.error(f"Krytyczny błąd w cyklu egzekucji: {e}", exc_info=True)

    async def _cleanup(self) -> None:
        """Bezpieczne zamknięcie połączeń (Graceful Shutdown)."""
        self._is_running = False
        await self._exchange.close()
        self._logger.info("FortressBot wyłączony bezpiecznie.")
