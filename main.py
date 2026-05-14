import asyncio
import sys
from src.config import AppConfig
from src.logger import CustomLogger
from src.exchange import BitgetExchange
from src.market_data import MarketDataService
from src.strategy import StrategyEngine
from src.risk import RiskManager
from src.execution import ExecutionManager
from src.state import StateManager
from src.bot import FortressBot, setup_signal_handlers

async def main():
    """
    Application Entry Point.
    Implements Dependency Injection (DI) manually to orchestrate the Fortress system.
    """
    # 1. Initialize Configuration (Fail-fast validation)
    try:
        config = AppConfig()
    except Exception as e:
        print(f"CRITICAL: Configuration error - {e}")
        sys.exit(1)

    # 2. Initialize Core Logger (Non-blocking JSON)
    logger = CustomLogger(name="FortressBot", log_file="fortress.log")
    logger.info("Fortress Architecture: Booting System...")

    try:
        # 3. Initialize Exchange Interface (REST)
        exchange = BitgetExchange(config, logger)

        # 4. Initialize State Manager (User Data Stream)
        state_manager = StateManager(config, logger, exchange)

        # 5. Initialize Market Data Service (K-line Stream)
        market_data = MarketDataService(config, logger, exchange)

        # 6. Initialize Strategy Engine (Pure Logic)
        strategy = StrategyEngine(config, logger)

        # 7. Initialize Risk Manager (Capital Guard)
        risk_manager = RiskManager(config, logger)

        # 8. Initialize Execution Manager (Muscle)
        execution = ExecutionManager(exchange, logger, state_manager)

        # 9. Inject dependencies into the Orchestrator
        bot = FortressBot(
            config=config,
            logger=logger,
            exchange=exchange,
            market_data=market_data,
            strategy=strategy,
            risk=risk_manager,
            execution=execution,
            state=state_manager
        )

        # 10. Setup OS Signal Handlers (Graceful Shutdown)
        loop = asyncio.get_running_loop()
        setup_signal_handlers(bot, loop)

        # 11. Run the Engine
        await bot.run()

    except Exception as e:
        logger.critical(f"Unhandled Boot Exception: {e}")
    finally:
        logger.info("System process terminated.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
