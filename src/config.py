from typing import List, Literal
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from .interfaces import IConfig

class AppConfig(BaseSettings, IConfig):
    """
    Configuration loader using Pydantic for validation.
    Loads from environment variables or a .env file.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # API Credentials
    BITGET_API_KEY: str = Field(..., description="Bitget API Key")
    BITGET_API_SECRET: str = Field(..., description="Bitget API Secret")
    BITGET_API_PASSPHRASE: str = Field(..., description="Bitget API Passphrase")

    # Trading Parameters
    SYMBOL: str = Field("SUI/USDT:USDT", description="Trading pair symbol")
    LEVERAGE: int = Field(20, ge=1, le=50)
    RISK_PERCENT: float = Field(3.0, ge=0.1, le=10.0)
    MAX_DAILY_DRAWDOWN: float = Field(0.03, ge=0.01, le=0.20)
    DRY_RUN: bool = Field(False)

    # Technical Parameters
    BASE_TIMEFRAME: str = "15m"
    TREND_TIMEFRAMES: List[str] = ["1h", "4h"]

    @property
    def api_key(self) -> str:
        return self.BITGET_API_KEY

    @property
    def api_secret(self) -> str:
        return self.BITGET_API_SECRET

    @property
    def symbol(self) -> str:
        return self.SYMBOL

    @field_validator("SYMBOL")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if ":" not in v:
            raise ValueError("Symbol must be in 'BASE/QUOTE:SETTLE' format for futures")
        return v
