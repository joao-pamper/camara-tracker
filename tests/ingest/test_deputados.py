"""Tests for deputies ingestion."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import ingest_deputados
from camara_tracker.storage import DEPUTADOS_SCHEMA, read_parquet

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
NOW = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    path = FIXTURES_DIR / name
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


def make_settings(tmp_path: Path) -> Settings:
    """Build settings that do not sleep and keep data in tmp_path."""
    return Settings(data_dir=tmp_path, items_per_page=2, min_interval_s=0.0)


def test_ingest_deputados_two_pages(tmp_path: Path) -> None:
    """Two pages yield sorted, metadata-tagged rows."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("deputados_page_1.json")),
                httpx.Response(200, json=load_fixture("deputados_page_2.json")),
            ]
        )
        with CamaraClient(settings) as client:
            result = ingest_deputados(client, settings, max_pages=2, now=NOW)

    table = read_parquet(result.path)
    assert result.rows == 3
    assert result.ingested_at == NOW
    assert table.schema == DEPUTADOS_SCHEMA
    assert table.column("id").to_pylist() == [204379, 220593, 220714]
    assert table.column("_ingested_at").to_pylist() == [NOW, NOW, NOW]
    assert table.column("_legislatura").to_pylist() == [57, 57, 57]


def test_ingest_deputados_dedupes_last_wins(tmp_path: Path) -> None:
    """Duplicate ids collapse to one row using the last value."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados").mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_duplicate.json"))
        )
        with CamaraClient(settings) as client:
            result = ingest_deputados(client, settings, max_pages=1, now=NOW)

    table = read_parquet(result.path)
    names = dict(zip(table.column("id").to_pylist(), table.column("nome").to_pylist(), strict=True))
    assert result.rows == 2
    assert names == {220593: "Updated Deputado", 999001: "Outro Deputado"}


def test_ingest_deputados_is_idempotent(tmp_path: Path) -> None:
    """Re-running over the same pages produces identical rows."""
    settings = make_settings(tmp_path)
    responses = [
        httpx.Response(200, json=load_fixture("deputados_page_1.json")),
        httpx.Response(200, json=load_fixture("deputados_page_2.json")),
        httpx.Response(200, json=load_fixture("deputados_page_1.json")),
        httpx.Response(200, json=load_fixture("deputados_page_2.json")),
    ]

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados").mock(side_effect=responses)
        with CamaraClient(settings) as client:
            first = ingest_deputados(client, settings, max_pages=2, now=NOW)
            first_rows = read_parquet(first.path).to_pylist()
            second = ingest_deputados(client, settings, max_pages=2, now=NOW)

    assert read_parquet(second.path).to_pylist() == first_rows
    assert second.rows == first.rows == 3


def test_ingest_deputados_empty_writes_schema(tmp_path: Path) -> None:
    """An empty response still writes a schema-only file."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados").mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_empty.json"))
        )
        with CamaraClient(settings) as client:
            result = ingest_deputados(client, settings, max_pages=1, now=NOW)

    table = read_parquet(result.path)
    assert result.rows == 0
    assert table.num_rows == 0
    assert table.schema == DEPUTADOS_SCHEMA


def test_ingest_deputados_invalid_row_reports_id(tmp_path: Path) -> None:
    """Invalid rows fail with the row id in the error message."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados").mock(
            return_value=httpx.Response(200, json=load_fixture("deputados_invalid.json"))
        )
        with CamaraClient(settings) as client, pytest.raises(ValueError, match="999999"):
            ingest_deputados(client, settings, max_pages=1, now=NOW)
