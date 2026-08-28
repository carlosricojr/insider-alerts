from functools import lru_cache

from pydantic import Field, model_validator
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
    notification_transport_db: str = Field(
        default="data/research/notification_transport.db",
        alias="NOTIFICATION_TRANSPORT_DB",
    )
    notification_transport_policy_path: str = Field(
        default="docs/research/contracts/notification-transport-v1.json",
        alias="NOTIFICATION_TRANSPORT_POLICY_PATH",
    )

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
    market_data_rate_limit_per_second: float = Field(
        default=1.0,
        alias="MARKET_DATA_RATE_LIMIT_PER_SECOND",
        gt=0,
        le=5,
    )
    market_data_retry_attempts: int = Field(
        default=3,
        alias="MARKET_DATA_RETRY_ATTEMPTS",
        ge=1,
        le=10,
    )
    market_data_retry_min_seconds: float = Field(
        default=0.5,
        alias="MARKET_DATA_RETRY_MIN_SECONDS",
        ge=0,
    )
    market_data_retry_max_seconds: float = Field(
        default=3.0,
        alias="MARKET_DATA_RETRY_MAX_SECONDS",
        ge=0,
    )
    market_earnings_shock_drop_threshold: float = Field(
        default=0.08,
        alias="MARKET_EARNINGS_SHOCK_DROP_THRESHOLD",
        gt=0,
        lt=1,
    )
    ib_gateway_host: str = Field(default="127.0.0.1", alias="IB_GATEWAY_HOST")
    ib_gateway_port: int = Field(default=4001, alias="IB_GATEWAY_PORT", ge=1, le=65535)
    insider_ib_client_id: int = Field(default=171, alias="INSIDER_IB_CLIENT_ID", ge=0)

    database_path: str = Field(default="data/insider_alerts.db", alias="DATABASE_PATH")

    @model_validator(mode="after")
    def validate_retry_bounds(self) -> "Settings":
        if self.market_data_retry_min_seconds > self.market_data_retry_max_seconds:
            raise ValueError(
                "MARKET_DATA_RETRY_MIN_SECONDS cannot exceed MARKET_DATA_RETRY_MAX_SECONDS"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
