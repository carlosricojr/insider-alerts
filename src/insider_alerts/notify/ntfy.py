from __future__ import annotations

from dataclasses import dataclass

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from insider_alerts.config import Settings


class NtfyNotificationError(RuntimeError):
    """Raised when an NTFY notification cannot be delivered."""


@dataclass(slots=True)
class NtfyNotifier:
    settings: Settings

    def send(
        self,
        title: str,
        message: str,
        tags: list[str] | None = None,
        priority: int | None = None,
        click: str | None = None,
        icon: str | None = None,
        markdown: bool = True,
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

        def _post_once() -> None:
            with httpx.Client(timeout=self.settings.ntfy_timeout_seconds) as client:
                response = client.post(url, content=message.encode("utf-8"), headers=headers)
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
                    _post_once()
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


# TODO(sprint-2): Add additional notifier providers (email/slack/webhook).
