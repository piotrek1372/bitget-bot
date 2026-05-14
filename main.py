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
    # Zastosowanie keyword arguments chroni przed błędami kolejności parametrów
    
    exchange = BitgetExchange(config=config, logger=logger_service)
    
    market_data = MarketDataService(config=config, logger=logger_service, exchange=exchange)
    
    strategy = StrategyEngine(config=config, logger=logger_service)
    
    risk_manager = RiskManager(config=config, logger=logger_service)
    
    state_manager = StateManager(config=config, logger=logger_service, exchange=exchange)
    
   execution_manager = ExecutionManager(
    logger=logger_service, 
    exchange=exchange, 
    state_manager=state_manager
)

    
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
