"""Tests for the Câmara API HTTP client."""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from camara_tracker.api import ApiError, CamaraClient, RetryExhaustedError
from camara_tracker.config import Settings

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
DEPUTADOS_URL = f"{BASE_URL}/deputados"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
ALL_IDS = [220593, 204379, 220714, 220714]


class FakeClock:
    """Deterministic monotonic clock advanced by fake sleeps."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSleep:
    """Sleep replacement that records delays and advances a fake clock."""

    def __init__(self, clock: FakeClock) -> None:
        self.calls: list[float] = []
        self._clock = clock

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self._clock.advance(seconds)


def make_client(
    settings: Settings | None = None,
) -> tuple[CamaraClient, FakeClock, RecordingSleep]:
    """Build a client with injected time controls and no request delay."""
    clock = FakeClock()
    sleep = RecordingSleep(clock)
    effective_settings = settings or Settings(min_interval_s=0.0)
    client = CamaraClient(effective_settings, clock=clock, sleep=sleep)
    return client, clock, sleep


def load_fixture(name: str) -> dict[str, Any]:
    # Fixtures are based on verified real API responses; page 2 omits its next
    # link so pagination termination can be exercised without a third page.
    path = FIXTURES_DIR / name
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


def next_href(payload: dict[str, Any]) -> str:
    """Return the href of the next link in a fixture."""
    links: list[dict[str, Any]] = payload["links"]
    return str(next(link["href"] for link in links if link["rel"] == "next"))


def test_get_returns_json_and_sends_headers() -> None:
    """get returns parsed JSON with the required request headers."""
    payload = load_fixture("deputados_page_1.json")

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(return_value=httpx.Response(200, json=payload))
        client, _, sleep = make_client()
        with client:
            result = client.get("deputados")

    assert result == payload
    request = route.calls[0].request
    assert request.headers["accept"] == "application/json"
    assert request.headers["user-agent"] == "camara-tracker/0.1"
    assert sleep.calls == []


def test_iter_pages_follows_next_link_then_stops() -> None:
    """iter_pages follows a next href directly and stops at the last page."""
    settings = Settings(items_per_page=2, min_interval_s=0.0)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
                httpx.Response(200, json=load_fixture("deputados_page_2.json")),
            ]
        )
        client, _, _ = make_client(settings)
        with client:
            pages = list(client.iter_pages("deputados", {"idLegislatura": 57}))

    assert [item["id"] for page in pages for item in page] == ALL_IDS
    assert len(route.calls) == 2
    assert str(route.calls[1].request.url) == next_href(load_fixture("deputados_page_1.json"))


def test_iter_items_flattens_pages() -> None:
    """iter_items yields items from every page in order."""
    settings = Settings(items_per_page=2, min_interval_s=0.0)

    with respx.mock(assert_all_called=False) as router:
        router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
                httpx.Response(200, json=load_fixture("deputados_page_2.json")),
            ]
        )
        client, _, _ = make_client(settings)
        with client:
            items = list(client.iter_items("deputados", {"idLegislatura": 57}))

    assert [item["id"] for item in items] == ALL_IDS


def test_first_request_sets_itens_and_pagina() -> None:
    """The first pagination request sets itens and pagina explicitly."""
    settings = Settings(items_per_page=2, min_interval_s=0.0)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_page_1.json"))
        )
        client, _, _ = make_client(settings)
        with client:
            list(client.iter_pages("deputados", {"idLegislatura": 57}, max_pages=1))

    params = route.calls[0].request.url.params
    assert params["idLegislatura"] == "57"
    assert params["itens"] == "2"
    assert params["pagina"] == "1"


def test_max_pages_limits_requests() -> None:
    """max_pages stops pagination even when a next link exists."""
    settings = Settings(items_per_page=2, min_interval_s=0.0)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_page_1.json"))
        )
        client, _, _ = make_client(settings)
        with client:
            pages = list(client.iter_pages("deputados", max_pages=1))

    assert len(pages) == 1
    assert len(route.calls) == 1


def test_empty_dados_stops_iteration() -> None:
    """An empty dados list ends pagination without yielding a page."""
    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_empty.json"))
        )
        client, _, _ = make_client()
        with client:
            pages = list(client.iter_pages("deputados"))

    assert pages == []
    assert len(route.calls) == 1


def test_iter_items_unpaged_sends_no_itens_or_pagina() -> None:
    """Endpoints that reject pagination params can be read with one plain request."""
    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/votacoes/v1/votos").mock(
            return_value=httpx.Response(200, json={"dados": [{"id": 1}], "links": []})
        )
        client, _, _ = make_client()
        with client:
            items = list(
                client.iter_items("/votacoes/v1/votos", {"uf": "SP"}, paginate=False)
            )

    assert items == [{"id": 1}]
    assert len(route.calls) == 1
    params = route.calls[0].request.url.params
    assert params["uf"] == "SP"
    assert "itens" not in params
    assert "pagina" not in params


def test_503_then_200_succeeds() -> None:
    """A server error is retried with exponential backoff and jitter."""
    settings = Settings(
        max_retries=1,
        backoff_base_s=0.01,
        backoff_max_s=1.0,
        min_interval_s=0.0,
    )

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
            ]
        )
        client, _, sleep = make_client(settings)
        with client:
            result = client.get("deputados")

    assert len(result["dados"]) == 2
    assert len(route.calls) == 2
    assert 0.01 <= sleep.calls[0] <= 0.06


def test_429_retry_after_drives_delay() -> None:
    """Retry-After seconds are used directly for 429 responses."""
    settings = Settings(max_retries=1, min_interval_s=0.0)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "2"}),
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
            ]
        )
        client, _, sleep = make_client(settings)
        with client:
            client.get("deputados")

    assert len(route.calls) == 2
    assert sleep.calls == pytest.approx([2.0])


def test_timeout_then_success() -> None:
    """A transport timeout is retried and can succeed."""
    settings = Settings(
        max_retries=1,
        backoff_base_s=0.01,
        backoff_max_s=1.0,
        min_interval_s=0.0,
    )

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.TimeoutException("timeout"),
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
            ]
        )
        client, _, sleep = make_client(settings)
        with client:
            result = client.get("deputados")

    assert len(result["dados"]) == 2
    assert len(route.calls) == 2
    assert 0.01 <= sleep.calls[0] <= 0.06


def test_retries_exhausted() -> None:
    """Retryable failures raise RetryExhaustedError after max_retries."""
    settings = Settings(
        max_retries=5,
        backoff_base_s=0.001,
        backoff_max_s=0.01,
        min_interval_s=0.0,
    )

    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(
            side_effect=[httpx.Response(503) for _ in range(settings.max_retries + 1)]
        )
        client, _, sleep = make_client(settings)
        with client, pytest.raises(RetryExhaustedError) as exc_info:
            client.get("deputados")

    assert exc_info.value.status_code == 503
    assert len(route.calls) == settings.max_retries + 1
    assert len(sleep.calls) == settings.max_retries


def test_404_raises_immediately() -> None:
    """Other 4xx responses raise ApiError without retrying."""
    with respx.mock(assert_all_called=False) as router:
        route = router.get(DEPUTADOS_URL).mock(return_value=httpx.Response(404))
        client, _, sleep = make_client()
        with client, pytest.raises(ApiError) as exc_info:
            client.get("deputados")

    assert exc_info.value.status_code == 404
    assert len(route.calls) == 1
    assert sleep.calls == []


def test_throttle_sleeps_remaining_interval() -> None:
    """Back-to-back requests sleep for the remaining minimum interval."""
    settings = Settings(min_interval_s=0.2, max_retries=0)

    with respx.mock(assert_all_called=False) as router:
        router.get(DEPUTADOS_URL).mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
                httpx.Response(200, json=load_fixture("deputados_page_2.json")),
            ]
        )
        client, _, sleep = make_client(settings)
        with client:
            client.get("deputados")
            client.get("deputados")

    assert sleep.calls == pytest.approx([0.2])
