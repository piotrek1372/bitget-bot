import json
import logging
import logging.handlers
import queue
from typing import Any, Dict, Optional
from datetime import datetime
from .interfaces import ILogger

class JsonFormatter(logging.Formatter):
    """Formats log records as JSON strings for structured logging."""
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
        }
        
        # Merge extra fields if they exist
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            log_record.update(record.extra_data)
            
        return json.dumps(log_record)

class CustomLogger(ILogger):
    """
    Non-blocking structured JSON logger.
    Uses QueueHandler + QueueListener to offload I/O to a background thread,
    ensuring the asyncio event loop is never blocked by disk/console writes.
    """
    def __init__(self, name: str = "FortressBot", log_file: str = "bot.log") -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        
        # 1. Create a thread-safe queue for log records
        self._log_queue: queue.Queue = queue.Queue(-1)  # Unlimited size
        
        # 2. Setup the internal handler that just puts items into the queue (Non-blocking)
        queue_handler = logging.handlers.QueueHandler(self._log_queue)
        self._logger.addHandler(queue_handler)
        
        # 3. Setup the real handlers (Console & File) that will run in the Listener thread
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_file)
        
        formatter = JsonFormatter()
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # 4. Start the Listener in a separate background thread
        self._listener = logging.handlers.QueueListener(
            self._log_queue, 
            console_handler, 
            file_handler, 
            respect_handler_level=True
        )
        self._listener.start()

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.info(message, extra={"extra_data": extra} if extra else None)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.error(message, extra={"extra_data": extra} if extra else None)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.warning(message, extra={"extra_data": extra} if extra else None)

    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._logger.critical(message, extra={"extra_data": extra} if extra else None)

    def stop(self) -> None:
        """Stops the background listener thread."""
        self._listener.stop()
