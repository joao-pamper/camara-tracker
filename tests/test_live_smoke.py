"""Opt-in live smoke tests against the public Câmara API."""

from pathlib import Path

import pytest

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import ingest_partidos


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
