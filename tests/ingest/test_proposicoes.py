"""Tests for deputy-authored proposition ingestion and compaction."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.proposicoes import (
    compact_proposicoes,
    ingest_proposicoes,
    is_fresh,
    shard_path,
)
from camara_tracker.models import Proposicao
from camara_tracker.storage import (
    PROPOSICOES_SCHEMA,
    read_parquet,
    records_to_table,
    write_parquet,
)

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
TODAY = date(2026, 1, 1)
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


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


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    payload: dict[str, Any] = json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))
    return payload


def make_settings(tmp_path: Path) -> Settings:
    """Build settings with no retry delays or request throttling."""
    return Settings(data_dir=tmp_path, items_per_page=2, min_interval_s=0.0, max_retries=0)


def make_client(settings: Settings) -> CamaraClient:
    """Build a client with fake sleep and clock."""
    clock = FakeClock()
    return CamaraClient(settings, sleep=RecordingSleep(clock), clock=clock)


def write_shard(
    settings: Settings,
    ano: int,
    deputado_id: int,
    ingested_at: datetime,
    records: list[Proposicao] | None = None,
) -> Path:
    """Write one shard with the same metadata used by ingestion."""
    values = records or [Proposicao(id=1, ano=ano)]
    table = records_to_table(
        values,
        PROPOSICOES_SCHEMA,
        ingested_at,
        settings.legislatura,
        constants={"_deputado_id": deputado_id, "_ano": ano},
    )
    metadata = dict(table.schema.metadata or {})
    metadata[b"_ingested_at"] = ingested_at.isoformat().encode("utf-8")
    path = shard_path(settings, ano, deputado_id)
    write_parquet(table.replace_schema_metadata(metadata), path)
    return path


def test_shard_path(tmp_path: Path) -> None:
    """Shards are partitioned by year and deputy id."""
    settings = make_settings(tmp_path)

    assert shard_path(settings, 2024, 62881) == (
        tmp_path / "bronze" / "proposicoes" / "_shards" / "ano=2024" / "deputado=62881.parquet"
    )


def test_is_fresh(tmp_path: Path) -> None:
    """Freshness uses file existence, closed years, and shard metadata."""
    settings = make_settings(tmp_path)
    missing = shard_path(settings, 2026, 1)
    closed = write_shard(settings, 2024, 1, NOW - timedelta(days=365))
    fresh = write_shard(settings, 2026, 2, NOW - timedelta(hours=1))
    stale = write_shard(settings, 2026, 3, NOW - timedelta(hours=25))

    assert not is_fresh(missing, 2026, TODAY, settings, NOW)
    assert is_fresh(closed, 2024, TODAY, settings, NOW)
    assert is_fresh(fresh, 2026, TODAY, settings, NOW)
    assert not is_fresh(stale, 2026, TODAY, settings, NOW)


def test_ingest_proposicoes_paginates_and_writes_shard(tmp_path: Path) -> None:
    """All pages of one deputy-year are stored in a typed shard."""
    settings = make_settings(tmp_path)
    progress: list[tuple[int, int]] = []

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/proposicoes").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("proposicoes_page_1.json")),
                httpx.Response(200, json=load_fixture("proposicoes_page_2.json")),
            ]
        )
        with make_client(settings) as client:
            result = ingest_proposicoes(
                client,
                settings,
                years=[2024],
                deputy_ids=[1],
                today=TODAY,
                now=NOW,
                progress=lambda done, total: progress.append((done, total)),
            )

    assert route.call_count == 2
    params = route.calls[0].request.url.params
    assert params["idDeputadoAutor"] == "1"
    assert params["ano"] == "2024"
    assert params["itens"] == "2"
    assert params["pagina"] == "1"
    assert result.fetched == 1
    assert result.skipped == 0
    assert result.failed == []
    assert result.rows == 3
    assert progress == [(1, 1)]

    table = read_parquet(shard_path(settings, 2024, 1))
    assert table.schema.equals(PROPOSICOES_SCHEMA, check_metadata=False)
    assert table.column("id").to_pylist() == [2418080, 2418081, 2418082]
    assert table.column("_deputado_id").to_pylist() == [1, 1, 1]
    assert table.column("_ano").to_pylist() == [2024, 2024, 2024]
    assert table.column("_ingested_at").to_pylist() == [NOW] * 3


def test_closed_year_skips_all_requests_and_force_refetches(tmp_path: Path) -> None:
    """A closed-year shard is fetched once and only force refetches it."""
    settings = make_settings(tmp_path)
    path = shard_path(settings, 2024, 1)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/proposicoes").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("proposicoes_page_1.json")),
                httpx.Response(200, json=load_fixture("proposicoes_page_2.json")),
            ]
        )
        with make_client(settings) as client:
            first = ingest_proposicoes(
                client, settings, years=[2024], deputy_ids=[1], today=TODAY, now=NOW
            )
        assert route.call_count == 2

    with respx.mock(assert_all_called=False) as router:
        with make_client(settings) as client:
            second = ingest_proposicoes(
                client, settings, years=[2024], deputy_ids=[1], today=TODAY, now=NOW
            )
        assert len(router.calls) == 0

    assert first.fetched == 1
    assert second.fetched == 0
    assert second.skipped == 1

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/proposicoes").mock(
            return_value=httpx.Response(200, json=load_fixture("proposicoes_empty.json"))
        )
        with make_client(settings) as client:
            forced = ingest_proposicoes(
                client,
                settings,
                years=[2024],
                deputy_ids=[1],
                force=True,
                today=TODAY,
                now=NOW,
            )

    assert route.call_count == 1
    assert forced.fetched == 1
    assert read_parquet(path).num_rows == 0


def test_empty_response_writes_fresh_empty_shard(tmp_path: Path) -> None:
    """An empty shard is a valid marker that prevents another request."""
    settings = make_settings(tmp_path)
    path = shard_path(settings, 2026, 999999)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/proposicoes").mock(
            return_value=httpx.Response(200, json=load_fixture("proposicoes_empty.json"))
        )
        with make_client(settings) as client:
            first = ingest_proposicoes(
                client, settings, years=[2026], deputy_ids=[999999], today=TODAY, now=NOW
            )
            second = ingest_proposicoes(
                client, settings, years=[2026], deputy_ids=[999999], today=TODAY, now=NOW
            )

    assert route.call_count == 1
    assert first.rows == 0
    assert second.skipped == 1
    table = read_parquet(path)
    assert table.num_rows == 0
    assert table.schema.equals(PROPOSICOES_SCHEMA, check_metadata=False)


def test_failure_is_isolated_per_deputy_year(tmp_path: Path) -> None:
    """One failed deputy-year is recorded while other shards still load."""
    settings = make_settings(tmp_path)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params["idDeputadoAutor"] == "1":
            return httpx.Response(500)
        return httpx.Response(
            200,
            json={
                "dados": load_fixture("proposicoes_page_1.json")["dados"][:1],
                "links": [],
            },
        )

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/proposicoes").mock(side_effect=respond)
        with make_client(settings) as client:
            result = ingest_proposicoes(
                client, settings, years=[2024], deputy_ids=[1, 2], today=TODAY, now=NOW
            )

    assert result.fetched == 1
    assert result.rows == 1
    assert len(result.failed) == 1
    failed_id, failed_year, message = result.failed[0]
    assert (failed_id, failed_year) == (1, 2024)
    assert "HTTP 500" in message
    assert not shard_path(settings, 2024, 1).exists()
    assert shard_path(settings, 2024, 2).exists()


def test_invalid_row_reports_deputy_year_and_index(tmp_path: Path) -> None:
    """Invalid API rows identify the deputy, year, and row index."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/proposicoes").mock(
            return_value=httpx.Response(200, json=load_fixture("proposicoes_invalid.json"))
        )
        with make_client(settings) as client:
            result = ingest_proposicoes(
                client, settings, years=[2024], deputy_ids=[62881], today=TODAY, now=NOW
            )

    assert len(result.failed) == 1
    assert "deputado 62881 ano 2024 row 0" in result.failed[0][2]


def test_compact_sorts_and_keeps_one_row_per_author(tmp_path: Path) -> None:
    """Compaction keeps a shared proposition once per deputy and validates the schema."""
    settings = make_settings(tmp_path)
    write_shard(
        settings,
        2024,
        2,
        NOW,
        [Proposicao(id=2418080, ano=2024), Proposicao(id=2418081, ano=2024)],
    )
    write_shard(
        settings,
        2024,
        1,
        NOW,
        [Proposicao(id=2418082, ano=2024), Proposicao(id=2418080, ano=2024)],
    )

    results = compact_proposicoes(settings, years=[2024])

    path = tmp_path / "bronze" / "proposicoes" / "proposicoes_2024.parquet"
    assert len(results) == 1
    assert results[0].path == path
    assert results[0].rows == 4
    table = read_parquet(path)
    assert table.schema.equals(PROPOSICOES_SCHEMA, check_metadata=False)
    assert table.column("_deputado_id").to_pylist() == [1, 1, 2, 2]
    assert table.column("id").to_pylist() == [2418080, 2418082, 2418080, 2418081]


def test_compact_without_shards_fails_clearly(tmp_path: Path) -> None:
    """Compaction reports the missing year instead of writing an empty file."""
    settings = make_settings(tmp_path)

    with pytest.raises(FileNotFoundError, match="No proposicoes shards for ano 2024"):
        compact_proposicoes(settings, years=[2024])


def test_limit_deputies_requires_non_default_data_dir() -> None:
    """A partial proposições run refuses the default data directory."""
    settings = Settings(data_dir=Path("data"))

    with (
        make_client(settings) as client,
        pytest.raises(ValueError, match="limit options require a non-default --data-dir"),
    ):
        ingest_proposicoes(client, settings, years=[2024], deputy_ids=[1], limit_deputies=1)
