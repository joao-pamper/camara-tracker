"""Opt-in live smoke tests against the public Câmara API."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import ingest_despesas, ingest_partidos, shard_path


@pytest.mark.live
def test_live_deputados() -> None:
    """Fetch one page of current deputies and verify item ids are present."""
    with CamaraClient() as client:
        pages = client.iter_pages(
            "deputados",
            {"idLegislatura": 57},
            max_pages=1,
        )
        items = [item for page in pages for item in page]

    assert any("id" in item for item in items)


@pytest.mark.live
def test_live_ingest_partidos(tmp_path: Path) -> None:
    """Ingest one live page of parties and verify a Parquet file is written."""
    settings = Settings(data_dir=tmp_path)

    with CamaraClient() as client:
        result = ingest_partidos(client, settings, max_pages=1)

    assert result.rows > 0
    assert result.path.exists()


@pytest.mark.live
def test_live_ingest_despesas(tmp_path: Path) -> None:
    """Ingest one live expense page and verify its shard is written."""
    settings = Settings(data_dir=tmp_path, min_interval_s=0.0)

    with CamaraClient(settings) as client:
        result = ingest_despesas(
            client,
            settings,
            years=[2024],
            deputy_ids=[62881],
            limit_deputies=1,
            max_pages=1,
            now=datetime.now(tz=UTC),
        )

    path = shard_path(settings, 2024, 62881)
    assert result.rows > 0
    assert path.exists()
