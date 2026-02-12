from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    ntfy_base_url: str = Field(default="https://ntfy.sh", alias="NTFY_BASE_URL")
    ntfy_topic: str = Field(default="insider-alerts", alias="NTFY_TOPIC")
    ntfy_token: str | None = Field(default=None, alias="NTFY_TOKEN")

    ntfy_timeout_seconds: float = Field(default=10.0, alias="NTFY_TIMEOUT_SECONDS", gt=0)
    ntfy_retry_attempts: int = Field(default=3, alias="NTFY_RETRY_ATTEMPTS", ge=1)
    ntfy_retry_min_seconds: float = Field(default=0.5, alias="NTFY_RETRY_MIN_SECONDS", ge=0)
    ntfy_retry_max_seconds: float = Field(default=3.0, alias="NTFY_RETRY_MAX_SECONDS", ge=0)

    sec_rss_url: str = Field(
        default="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&start=-1&count=40&output=rss",
        alias="SEC_RSS_URL",
    )
    sec_user_agent: str = Field(
        default="insider-alerts/0.2 (contact: sec-access@example.com)",
        alias="SEC_USER_AGENT",
    )
    sec_timeout_seconds: float = Field(default=15.0, alias="SEC_TIMEOUT_SECONDS", gt=0)
    sec_rate_limit_per_second: float = Field(
        default=5.0,
        alias="SEC_RATE_LIMIT_PER_SECOND",
        gt=0,
        le=10,
    )
    sec_retry_attempts: int = Field(default=4, alias="SEC_RETRY_ATTEMPTS", ge=1)
    sec_retry_min_seconds: float = Field(default=0.25, alias="SEC_RETRY_MIN_SECONDS", ge=0)
    sec_retry_max_seconds: float = Field(default=3.0, alias="SEC_RETRY_MAX_SECONDS", ge=0)
    market_context_enabled: bool = Field(default=False, alias="MARKET_CONTEXT_ENABLED")
    market_data_timeout_seconds: float = Field(
        default=10.0,
        alias="MARKET_DATA_TIMEOUT_SECONDS",
        gt=0,
    )
    market_earnings_shock_drop_threshold: float = Field(
        default=0.08,
        alias="MARKET_EARNINGS_SHOCK_DROP_THRESHOLD",
        gt=0,
        lt=1,
    )

    database_path: str = Field(default="data/insider_alerts.db", alias="DATABASE_PATH")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
