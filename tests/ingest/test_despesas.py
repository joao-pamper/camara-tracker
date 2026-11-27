"""Tests for deputy expense ingestion and compaction."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import compact_despesas, ingest_despesas, is_fresh, shard_path
from camara_tracker.models import Despesa
from camara_tracker.storage import (
    DESPESAS_SCHEMA,
    read_parquet,
    records_to_table,
    write_parquet,
)

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
TODAY = date(2026, 1, 1)
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    path = FIXTURES_DIR / name
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


def make_settings(tmp_path: Path) -> Settings:
    """Build settings that do not sleep or retry in tests."""
    return Settings(
        data_dir=tmp_path,
        items_per_page=2,
        min_interval_s=0.0,
        max_retries=0,
    )


def write_shard(
    settings: Settings,
    ano: int,
    deputado_id: int,
    ingested_at: datetime,
    records: list[Despesa] | None = None,
) -> Path:
    """Write one shard with the same metadata used by ingestion."""
    values = records or [Despesa(ano=ano, mes=1, valorLiquido=-1.0)]
    table = records_to_table(
        values,
        DESPESAS_SCHEMA,
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
        tmp_path / "bronze" / "despesas" / "_shards" / "ano=2024" / "deputado=62881.parquet"
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


def test_ingest_despesas_writes_2x2_shards(tmp_path: Path) -> None:
    """Two deputies and two years produce four complete shards."""
    settings = make_settings(tmp_path)
    progress: list[tuple[int, int]] = []

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_page_1.json"))
        )
        router.get(f"{BASE_URL}/deputados/2/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_page_1.json"))
        )
        with CamaraClient(settings) as client:
            result = ingest_despesas(
                client,
                settings,
                years=[2024, 2025],
                deputy_ids=[2, 1],
                today=TODAY,
                now=NOW,
                max_pages=1,
                progress=lambda done, total: progress.append((done, total)),
            )

    assert result.fetched == 4
    assert result.skipped == 0
    assert result.failed == []
    assert result.rows == 8
    assert progress == [(1, 4), (2, 4), (3, 4), (4, 4)]

    for deputado_id in (1, 2):
        for ano in (2024, 2025):
            table = read_parquet(shard_path(settings, ano, deputado_id))
            assert table.schema.equals(DESPESAS_SCHEMA, check_metadata=False)
            assert table.num_rows == 2
            assert table.column("_deputado_id").to_pylist() == [deputado_id, deputado_id]
            assert table.column("_ano").to_pylist() == [ano, ano]
            assert table.column("_ingested_at").to_pylist() == [NOW, NOW]


def test_closed_year_shard_is_not_refetched(tmp_path: Path) -> None:
    """A complete closed-year shard avoids all HTTP requests."""
    settings = make_settings(tmp_path)
    path = write_shard(settings, 2024, 1, NOW - timedelta(days=365))

    with respx.mock(assert_all_called=False), CamaraClient(settings) as client:
        result = ingest_despesas(
            client,
            settings,
            years=[2024],
            deputy_ids=[1],
            today=TODAY,
            now=NOW,
        )

    assert result.fetched == 0
    assert result.skipped == 1
    assert path.exists()


def test_force_refetches_fresh_shard(tmp_path: Path) -> None:
    """Force replaces a shard even when it is marked fresh."""
    settings = make_settings(tmp_path)
    path = write_shard(settings, 2024, 1, NOW - timedelta(days=365))

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/deputados/1/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_page_1.json"))
        )
        with CamaraClient(settings) as client:
            result = ingest_despesas(
                client,
                settings,
                years=[2024],
                deputy_ids=[1],
                force=True,
                today=TODAY,
                now=NOW,
                max_pages=1,
            )

    assert route.call_count == 1
    assert result.fetched == 1
    assert result.skipped == 0
    assert read_parquet(path).num_rows == 2


def test_empty_response_writes_fresh_empty_shard(tmp_path: Path) -> None:
    """An empty shard is still a valid, complete marker."""
    settings = make_settings(tmp_path)
    path = shard_path(settings, 2026, 999999)
    payload = load_fixture("despesas_empty.json")

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/deputados/999999/despesas").mock(
            return_value=httpx.Response(200, json=payload)
        )
        with CamaraClient(settings) as client:
            first = ingest_despesas(
                client,
                settings,
                years=[2026],
                deputy_ids=[999999],
                today=TODAY,
                now=NOW,
            )
            second = ingest_despesas(
                client,
                settings,
                years=[2026],
                deputy_ids=[999999],
                today=TODAY,
                now=NOW,
            )

    assert route.call_count == 1
    assert first.rows == 0
    assert second.fetched == 0
    assert second.skipped == 1
    table = read_parquet(path)
    assert table.num_rows == 0
    assert table.schema.equals(DESPESAS_SCHEMA, check_metadata=False)


def test_failure_is_isolated_per_deputy_year(tmp_path: Path) -> None:
    """One failed deputy-year is recorded while other shards still load."""
    settings = make_settings(tmp_path)
    success = httpx.Response(200, json=load_fixture("despesas_page_1.json"))

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/despesas").mock(return_value=httpx.Response(500))
        router.get(f"{BASE_URL}/deputados/2/despesas").mock(return_value=success)
        router.get(f"{BASE_URL}/deputados/3/despesas").mock(return_value=success)
        with CamaraClient(settings) as client:
            result = ingest_despesas(
                client,
                settings,
                years=[2024],
                deputy_ids=[1, 2, 3],
                today=TODAY,
                now=NOW,
                max_pages=1,
            )

    assert result.fetched == 2
    assert result.rows == 4
    assert len(result.failed) == 1
    failed_id, failed_year, message = result.failed[0]
    assert (failed_id, failed_year) == (1, 2024)
    assert "HTTP 500" in message
    assert not shard_path(settings, 2024, 1).exists()
    assert shard_path(settings, 2024, 2).exists()
    assert shard_path(settings, 2024, 3).exists()


def test_invalid_row_reports_deputy_year_and_index(tmp_path: Path) -> None:
    """Invalid API rows identify the deputy, year, and row index."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/62881/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_invalid.json"))
        )
        with CamaraClient(settings) as client:
            result = ingest_despesas(
                client,
                settings,
                years=[2024],
                deputy_ids=[62881],
                today=TODAY,
                now=NOW,
            )

    assert len(result.failed) == 1
    assert "deputado 62881 ano 2024 row 0" in result.failed[0][2]


def test_missing_deputados_file_has_actionable_error(tmp_path: Path) -> None:
    """A missing deputies file tells the user which command to run."""
    settings = make_settings(tmp_path)

    with (
        CamaraClient(settings) as client,
        pytest.raises(FileNotFoundError, match="ingest deputados"),
    ):
        ingest_despesas(client, settings, years=[2024])


def test_compact_despesas_merges_and_sorts(tmp_path: Path) -> None:
    """Compaction writes one sorted file with nulls last."""
    settings = make_settings(tmp_path)
    write_shard(
        settings,
        2024,
        1,
        NOW,
        [
            Despesa(ano=2024, mes=3, dataDocumento="2024-03-01T00:00:00"),
            Despesa(ano=2024, mes=None, dataDocumento="2024-01-01T00:00:00"),
            Despesa(ano=2024, mes=3, dataDocumento=None),
            Despesa(ano=2024, mes=1, dataDocumento="2024-01-01T00:00:00"),
        ],
    )
    write_shard(
        settings,
        2024,
        2,
        NOW,
        [
            Despesa(ano=2024, mes=2, dataDocumento="2024-02-01T00:00:00"),
            Despesa(ano=2024, mes=1, dataDocumento=None),
        ],
    )

    results = compact_despesas(settings, years=[2024])

    assert len(results) == 1
    path = tmp_path / "bronze" / "despesas" / "despesas_2024.parquet"
    assert results[0].path == path
    table = read_parquet(path)
    assert results[0].rows == 6
    assert table.schema.equals(DESPESAS_SCHEMA, check_metadata=False)
    assert table.column("_deputado_id").to_pylist() == [1, 1, 1, 1, 2, 2]
    assert table.column("mes").to_pylist() == [1, 3, 3, None, 1, 2]
    assert table.column("dataDocumento").to_pylist() == [
        "2024-01-01T00:00:00",
        "2024-03-01T00:00:00",
        None,
        "2024-01-01T00:00:00",
        None,
        "2024-02-01T00:00:00",
    ]


def test_limit_deputies_requires_non_default_data_dir() -> None:
    """A partial despesas run refuses the default data directory."""
    settings = Settings(data_dir=Path("data"))

    with (
        CamaraClient(settings) as client,
        pytest.raises(ValueError, match="limit options require a non-default --data-dir"),
    ):
        ingest_despesas(
            client,
            settings,
            years=[2024],
            deputy_ids=[1],
            limit_deputies=1,
        )


def test_compact_despesas_without_shards_fails_clearly(tmp_path: Path) -> None:
    """Compaction fails when a year has no shards."""
    settings = make_settings(tmp_path)

    with pytest.raises(FileNotFoundError, match="No despesas shards for ano 2024"):
        compact_despesas(settings, years=[2024])
