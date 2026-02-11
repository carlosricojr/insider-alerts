from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from insider_alerts.config import Settings


class SecHttpError(RuntimeError):
    """Raised when SEC requests fail after retries."""


class SecRetryableStatusError(SecHttpError):
    """Retryable status-code failure."""


@dataclass(slots=True)
class SecHttpClient:
    settings: Settings
    now_fn: Callable[[], float] = time.monotonic
    sleep_fn: Callable[[float], None] = time.sleep
    _last_request_ts: float = field(default=0.0, init=False)

    def _enforce_rate_limit(self) -> None:
        interval = 1.0 / self.settings.sec_rate_limit_per_second
        now = self.now_fn()
        elapsed = now - self._last_request_ts
        if elapsed < interval:
            self.sleep_fn(interval - elapsed)
        self._last_request_ts = self.now_fn()

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.settings.sec_user_agent,
            "Accept-Encoding": "gzip, deflate",
        }

    def _get_once(self, url: str) -> str:
        self._enforce_rate_limit()
        with httpx.Client(
            timeout=self.settings.sec_timeout_seconds,
            headers=self._headers(),
        ) as client:
            response = client.get(url)
        if response.status_code in {403, 429} or response.status_code >= 500:
            raise SecRetryableStatusError(f"retryable status code: {response.status_code}")
        if response.status_code >= 400:
            raise SecHttpError(f"non-retryable status code: {response.status_code}")
        return response.text

    def get_text(self, url: str) -> str:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(self.settings.sec_retry_attempts),
                wait=wait_exponential_jitter(
                    initial=self.settings.sec_retry_min_seconds,
                    max=self.settings.sec_retry_max_seconds,
                ),
                retry=retry_if_exception_type((httpx.HTTPError, SecRetryableStatusError)),
                reraise=True,
            ):
                with attempt:
                    return self._get_once(url)
        except (httpx.HTTPError, SecRetryableStatusError, SecHttpError) as exc:
            raise SecHttpError(f"SEC request failed for {url}: {exc}") from exc

        raise SecHttpError(f"SEC request failed for {url}: unknown retry state")
