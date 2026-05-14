# main.py
import asyncio
import signal
import sys

from src.config import AppConfig
from src.logger import HerokuLogger
from src.exchange import BitgetExchange
from src.market_data import MarketDataService
from src.strategy import StrategyEngine
from src.risk import RiskManager
from src.execution import ExecutionManager
from src.state import StateManager
from src.bot import FortressBot

async def shutdown_sequence(loop: asyncio.AbstractEventLoop) -> None:
    """Obsługa SIGTERM/SIGINT z Heroku (limit 30 sekund)."""
    print("Otrzymano sygnał SIGTERM. Rozpoczynam Graceful Shutdown...", flush=True)
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

def main() -> None:
    # 1. Inicjalizacja konfiguracji (Fail-fast przy braku zmiennych .env)
    config = AppConfig()
    
    # 2. Inicjalizacja Loggera (sys.stdout dla Heroku Logplex)
    logger_service = HerokuLogger()
    
    # 3. Kontener Dependency Injection (Wstrzykiwanie zależności)
    exchange = BitgetExchange(config)
    market_data = MarketDataService(config, exchange)
    strategy = StrategyEngine(config)
    risk_manager = RiskManager(config)
    state_manager = StateManager(config, exchange)
    execution_manager = ExecutionManager(config, exchange, state_manager)
    
    # 4. Inicjalizacja głównego Orkiestratora
    bot = FortressBot(
        config=config,
        logger=logger_service,
        exchange=exchange,
        market_data=market_data,
        strategy=strategy,
        risk_manager=risk_manager,
        execution_manager=execution_manager,
        state_manager=state_manager
    )

    loop = asyncio.get_event_loop()
    
    # Rejestracja handlerów dla sygnałów systemowych (nie działa natywnie na Windowsie)
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown_sequence(loop)))

    try:
        loop.run_until_complete(bot.run())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
