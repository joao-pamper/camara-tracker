"""Synchronous HTTP client for the Câmara dos Deputados open data API."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator, Mapping
from types import TracebackType
from typing import Any, cast

import httpx

from camara_tracker.api.errors import ApiError, RetryExhaustedError
from camara_tracker.config import Settings

LOGGER: logging.Logger = logging.getLogger(__name__)


class CamaraClient:
    """Small synchronous client with throttling, retries, and pagination."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings or Settings()
        self._http_client = http_client or httpx.Client()
        self._owns_http_client = http_client is None
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None

    def __enter__(self) -> CamaraClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close an owned HTTP client."""
        if self._owns_http_client:
            self._http_client.close()

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Fetch one JSON object from the API."""
        url = self._absolute_url(path)
        for attempt in range(self._settings.max_retries + 1):
            self._throttle()
            try:
                response = self._http_client.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self._settings.timeout_s,
                )
            except httpx.TimeoutException as exc:
                self._retry_or_raise(
                    attempt=attempt,
                    url=url,
                    reason="request timeout",
                    status_code=None,
                    retry_after=None,
                    cause=exc,
                )
                continue
            except httpx.TransportError as exc:
                self._retry_or_raise(
                    attempt=attempt,
                    url=url,
                    reason=f"transport error: {exc}",
                    status_code=None,
                    retry_after=None,
                    cause=exc,
                )
                continue

            if self._is_retryable_status(response.status_code):
                retry_after = (
                    self._retry_after_seconds(response)
                    if response.status_code == 429
                    else None
                )
                self._retry_or_raise(
                    attempt=attempt,
                    url=url,
                    reason=f"HTTP {response.status_code}",
                    status_code=response.status_code,
                    retry_after=retry_after,
                    cause=None,
                )
                continue

            if response.status_code >= 400:
                raise ApiError(response.status_code, url, f"HTTP {response.status_code}")

            return self._parse_json(response, url)

        raise RetryExhaustedError(None, url, "Retry loop ended unexpectedly")

    def iter_pages(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        max_pages: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield each page's ``dados`` list, following ``next`` links."""
        next_url: str | None = None
        pages_seen = 0

        while max_pages is None or pages_seen < max_pages:
            if next_url is None:
                page_params: dict[str, Any] = dict(params or {})
                page_params["itens"] = self._settings.items_per_page
                page_params["pagina"] = 1
                payload = self.get(path, page_params)
            else:
                payload = self.get(next_url)

            items = self._dados(payload, path)
            if not items:
                return

            yield items
            pages_seen += 1
            next_url = self._next_link(payload)
            if next_url is None:
                return

    def iter_items(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield individual items across pages."""
        for page in self.iter_pages(path, params, max_pages=max_pages):
            yield from page

    def _absolute_url(self, path: str) -> str:
        if httpx.URL(path).is_absolute_url:
            return str(httpx.URL(path))
        return f"{self._settings.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": self._settings.user_agent,
        }

    def _throttle(self) -> None:
        now = self._clock()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self._settings.min_interval_s - elapsed
            if remaining > 0:
                self._sleep(remaining)
                now = self._clock()
        self._last_request_at = now

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code == 429 or status_code >= 500

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if raw is None:
            return None
        try:
            return max(0.0, float(raw))
        except ValueError:
            return None

    def _backoff_delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, self._settings.backoff_max_s)
        exponential: float = self._settings.backoff_base_s * (2**attempt)
        jitter: float = random.uniform(0.0, 0.05)
        delay: float = min(exponential + jitter, self._settings.backoff_max_s)
        return delay

    def _retry_or_raise(
        self,
        *,
        attempt: int,
        url: str,
        reason: str,
        status_code: int | None,
        retry_after: float | None,
        cause: Exception | None,
    ) -> None:
        if attempt >= self._settings.max_retries:
            raise RetryExhaustedError(
                status_code,
                url,
                f"Retries exhausted after {self._settings.max_retries} retries: {reason}",
            ) from cause
        delay = self._backoff_delay(attempt, retry_after)
        LOGGER.warning(
            "%s for %s; retrying attempt %d in %.3fs",
            reason,
            url,
            attempt + 1,
            delay,
        )
        self._sleep(delay)

    @staticmethod
    def _parse_json(response: httpx.Response, url: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiError(response.status_code, url, "Invalid JSON response") from exc
        if not isinstance(payload, dict):
            raise ApiError(response.status_code, url, "Expected a JSON object")
        return cast(dict[str, Any], payload)

    @staticmethod
    def _dados(payload: dict[str, Any], url: str) -> list[dict[str, Any]]:
        dados = payload.get("dados")
        if dados is None:
            return []
        if not isinstance(dados, list):
            raise ApiError(None, url, "Expected 'dados' to be a list")
        return [item for item in dados if isinstance(item, dict)]

    @staticmethod
    def _next_link(payload: dict[str, Any]) -> str | None:
        links = payload.get("links")
        if not isinstance(links, list):
            return None
        for link in links:
            if isinstance(link, dict) and link.get("rel") == "next":
                href = link.get("href")
                if isinstance(href, str) and href:
                    return href
        return None
