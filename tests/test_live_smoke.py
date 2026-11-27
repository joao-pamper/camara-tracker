"""Opt-in live smoke tests against the public Câmara API."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import (
    ingest_despesas,
    ingest_historico,
    ingest_partidos,
    ingest_proposicoes,
    ingest_votacoes,
    proposicoes_shard_path,
    shard_path,
    shard_paths,
)


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
def test_live_ingest_historico(tmp_path: Path) -> None:
    """Ingest one live deputy's status history and verify the bronze file."""
    settings = Settings(data_dir=tmp_path, min_interval_s=0.0)

    with CamaraClient(settings) as client:
        result = ingest_historico(
            client,
            settings,
            deputy_ids=[62881],
            limit_deputies=1,
            now=datetime.now(tz=UTC),
        )

    path = tmp_path / "bronze" / "deputados_historico.parquet"
    assert result.rows > 0
    assert path.exists()


@pytest.mark.live
def test_live_ingest_proposicoes(tmp_path: Path) -> None:
    """Ingest one live deputy-year of propositions and verify its shard."""
    settings = Settings(data_dir=tmp_path, min_interval_s=0.0)

    with CamaraClient(settings) as client:
        result = ingest_proposicoes(
            client,
            settings,
            years=[2024],
            deputy_ids=[62881],
            limit_deputies=1,
            now=datetime.now(tz=UTC),
        )

    path = proposicoes_shard_path(settings, 2024, 62881)
    assert result.rows > 0
    assert result.failed == []
    assert path.exists()


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


@pytest.mark.live
def test_live_ingest_votacoes(tmp_path: Path) -> None:
    """Ingest a small live month of votações and verify both shards are written."""
    settings = Settings(data_dir=tmp_path, min_interval_s=0.0)

    with CamaraClient(settings) as client:
        result = ingest_votacoes(
            client,
            settings,
            years=[2024],
            months=[3],
            limit_votacoes=2,
            now=datetime.now(tz=UTC),
        )

    assert result.months_fetched == 1
    assert result.votacoes_rows == 2
    assert result.failed == []
    for path in shard_paths(settings, 2024, 3):
        assert path.exists()
