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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
