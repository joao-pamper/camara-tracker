"""Tests for parties ingestion."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import ingest_partidos
from camara_tracker.storage import PARTIDOS_SCHEMA, read_parquet

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
NOW = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    path = FIXTURES_DIR / name
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


def test_ingest_partidos_two_pages(tmp_path: Path) -> None:
    """Two pages are validated, deduplicated, sorted, and stored."""
    settings = Settings(
        data_dir=tmp_path,
        legislatura=56,
        items_per_page=2,
        min_interval_s=0.0,
    )

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/partidos").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("partidos_page_1.json")),
                httpx.Response(200, json=load_fixture("partidos_page_2.json")),
            ]
        )
        with CamaraClient(settings) as client:
            result = ingest_partidos(client, settings, max_pages=2, now=NOW)

    table = read_parquet(result.path)
    assert result.rows == 4
    assert result.ingested_at == NOW
    assert table.schema == PARTIDOS_SCHEMA
    assert table.column("id").to_pylist() == [36898, 37905, 37906, 37915]
    assert table.column("_ingested_at").to_pylist() == [NOW, NOW, NOW, NOW]
    assert table.column("_legislatura").to_pylist() == [56, 56, 56, 56]
