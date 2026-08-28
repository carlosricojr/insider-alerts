from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from insider_alerts.config import Settings

_PROVIDER_MESSAGE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class NtfyNotificationError(RuntimeError):
    """Raised when an NTFY notification cannot be delivered."""


@dataclass(frozen=True, slots=True)
class NtfyTransportEvent:
    """Secret-free observation of one existing ntfy HTTP attempt."""

    attempt_number: int
    phase: Literal["request_started", "response_received", "transport_failed"]
    occurred_at_utc: datetime
    request_body_sha256: str
    route_sha256: str
    http_status: int | None = None
    response_body_sha256: str | None = None
    provider_message_id: str | None = None
    provider_message_time: int | None = None
    exception_class: str | None = None


@dataclass(slots=True)
class NtfyNotifier:
    settings: Settings
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC)

    def send(
        self,
        title: str,
        message: str,
        tags: list[str] | None = None,
        priority: int | None = None,
        click: str | None = None,
        icon: str | None = None,
        markdown: bool = True,
        observer: Callable[[NtfyTransportEvent], None] | None = None,
        observer_error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        """Send a notification to NTFY using configured topic and auth token."""
        url = f"{str(self.settings.ntfy_base_url).rstrip('/')}/{self.settings.ntfy_topic}"
        headers = self._build_headers(
            title=title,
            tags=tags,
            priority=priority,
            click=click,
            icon=icon,
            markdown=markdown,
        )

        body = message.encode("utf-8")
        body_sha = hashlib.sha256(body).hexdigest()
        route_sha = hashlib.sha256(url.encode("utf-8")).hexdigest()

        def _now() -> datetime:
            value = self.now_fn()
            if value.tzinfo is None:
                raise ValueError("ntfy transport clock returned a naive timestamp")
            return value.astimezone(UTC)

        def _emit(event: NtfyTransportEvent) -> None:
            if observer is None:
                return
            try:
                observer(event)
            except Exception as exc:
                if observer_error_handler is not None:
                    with suppress(Exception):
                        observer_error_handler(exc)

        def _post_once(attempt_number: int) -> None:
            requested_at = _now()
            _emit(
                NtfyTransportEvent(
                    attempt_number=attempt_number,
                    phase="request_started",
                    occurred_at_utc=requested_at,
                    request_body_sha256=body_sha,
                    route_sha256=route_sha,
                )
            )
            with httpx.Client(timeout=self.settings.ntfy_timeout_seconds) as client:
                try:
                    response = client.post(url, content=body, headers=headers)
                except httpx.HTTPError as exc:
                    _emit(
                        NtfyTransportEvent(
                            attempt_number=attempt_number,
                            phase="transport_failed",
                            occurred_at_utc=_now(),
                            request_body_sha256=body_sha,
                            route_sha256=route_sha,
                            exception_class=type(exc).__name__,
                        )
                    )
                    raise
            responded_at = _now()
            response_sha = hashlib.sha256(response.content).hexdigest()
            provider_id, provider_time = _provider_identity(response.content)
            _emit(
                NtfyTransportEvent(
                    attempt_number=attempt_number,
                    phase="response_received",
                    occurred_at_utc=responded_at,
                    request_body_sha256=body_sha,
                    route_sha256=route_sha,
                    http_status=response.status_code,
                    response_body_sha256=response_sha,
                    provider_message_id=provider_id,
                    provider_message_time=provider_time,
                )
            )
            response.raise_for_status()

        try:
            for attempt in Retrying(
                stop=stop_after_attempt(self.settings.ntfy_retry_attempts),
                wait=wait_exponential(
                    multiplier=1,
                    min=self.settings.ntfy_retry_min_seconds,
                    max=self.settings.ntfy_retry_max_seconds,
                ),
                retry=retry_if_exception_type((httpx.HTTPError,)),
                reraise=True,
            ):
                with attempt:
                    _post_once(attempt.retry_state.attempt_number)
        except httpx.HTTPError as exc:
            raise NtfyNotificationError(f"NTFY notification failed: {exc}") from exc

    def _build_headers(
        self,
        title: str,
        tags: list[str] | None,
        priority: int | None,
        click: str | None,
        icon: str | None,
        markdown: bool,
    ) -> dict[str, str]:
        headers: dict[str, str] = {
            "Title": title,
            "Markdown": "yes" if markdown else "no",
        }

        if tags:
            headers["Tags"] = ",".join(tags)
        if priority is not None:
            headers["Priority"] = str(priority)
        if click:
            headers["Click"] = click
        if icon:
            headers["Icon"] = icon
        if self.settings.ntfy_token:
            headers["Authorization"] = f"Bearer {self.settings.ntfy_token}"

        return headers


def _provider_identity(content: bytes) -> tuple[str | None, int | None]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None
    if not isinstance(payload, dict):
        return None, None
    identifier = payload.get("id")
    provider_id = (
        identifier
        if isinstance(identifier, str) and _PROVIDER_MESSAGE_ID.fullmatch(identifier)
        else None
    )
    raw_time = payload.get("time")
    provider_time = (
        raw_time
        if isinstance(raw_time, int) and not isinstance(raw_time, bool) and raw_time >= 0
        else None
    )
    return provider_id, provider_time


# TODO(sprint-2): Add additional notifier providers (email/slack/webhook).
