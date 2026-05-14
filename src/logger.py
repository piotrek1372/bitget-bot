import logging
import sys
import queue
from logging.handlers import QueueHandler, QueueListener
from typing import Any

# Jeśli masz zdefiniowany ILogger w src.interfaces, odkomentuj poniższą linię:
# from .interfaces import ILogger
# Jeśli nie, klasa HerokuLogger nie musi po nim jawnie dziedziczyć w Pythonie (tzw. Duck Typing).

class HerokuLogger:
    """
    Logger dedykowany dla środowiska Heroku (12-Factor App).
    Wypisuje logi asynchronicznie na sys.stdout w formacie JSON, 
    aby nie blokować głównej pętli asyncio.
    """
    
    def __init__(self, name: str = "FortressBot") -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        
        # Kolejka bez limitu wielkości
        self._log_queue = queue.Queue(-1)
        
        # Handler wkładający logi do kolejki (nieblokujący)
        queue_handler = QueueHandler(self._log_queue)
        self._logger.addHandler(queue_handler)
        
        # Zapis wyłącznie na standardowe wyjście (wymóg Heroku)
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Formatowanie JSON ułatwia analizę w zewnętrznych narzędziach podpiętych pod Heroku
        formatter = logging.Formatter(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}'
        )
        console_handler.setFormatter(formatter)
        
        # Listener działający w wątku w tle
        self._listener = QueueListener(self._log_queue, console_handler)
        self._listener.start()

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.critical(msg, *args, **kwargs)
        
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(msg, *args, **kwargs)
