from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import pandas as pd
from .models import MarketSignal, Position, OrderResult, SignalSide

class IConfig(ABC):
    """Interface for configuration management."""
    @property
    @abstractmethod
    def api_key(self) -> str: ...
    
    @property
    @abstractmethod
    def api_secret(self) -> str: ...
    
    @property
    @abstractmethod
    def symbol(self) -> str: ...

class ILogger(ABC):
    """Interface for structured logging."""
    @abstractmethod
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    
    @abstractmethod
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    
    @abstractmethod
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    
    @abstractmethod
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None: ...

class IExchange(ABC):
    """Interface for exchange interactions (REST)."""
    @abstractmethod
    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame: ...
    
    @abstractmethod
    async def create_market_order(self, symbol: str, side: str, amount: float, params: Dict[str, Any]) -> OrderResult: ...
    
    @abstractmethod
    async def fetch_balance(self) -> float: ...

class IMarketData(ABC):
    """Interface for WebSocket market data streams."""
    @abstractmethod
    async def start(self) -> None: ...
    
    @abstractmethod
    async def stop(self) -> None: ...
    
    @abstractmethod
    def get_latest_data(self) -> Dict[str, pd.DataFrame]: ...

    @abstractmethod
    def set_event_trigger(self, event: asyncio.Event) -> None: ...

class IStrategy(ABC):
    """Interface for decision logic."""
    @abstractmethod
    def calculate_signal(self, data: Dict[str, pd.DataFrame]) -> MarketSignal: ...

class IRiskManager(ABC):
    """Interface for capital protection and sizing."""
    @abstractmethod
    def validate_execution(self, signal: MarketSignal, balance: float) -> Optional[float]: ...
    
    @abstractmethod
    def check_circuit_breakers(self, current_equity: float) -> bool: ...

class IExecutionManager(ABC):
    """Interface for order formatting and dispatch."""
    @abstractmethod
    async def execute_signal(self, signal: MarketSignal, quantity: float) -> Optional[OrderResult]: ...

class IStateManager(ABC):
    """Interface for tracking account state via WebSocket."""
    @abstractmethod
    async def start(self) -> None: ...
    
    @abstractmethod
    async def stop(self) -> None: ...
    
    @property
    @abstractmethod
    def current_position(self) -> Optional[Position]: ...

    @property
    @abstractmethod
    def is_locked(self) -> bool: ...

    @abstractmethod
    def set_pending_lock(self, locked: bool) -> None: ...

    @abstractmethod
    async def watchdog_reconciliation(self, order_id: str, timeout: float = 3.0) -> None: ...
