"""Tests for shared ingestion helpers."""

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from camara_tracker.ingest.common import (
    IngestResult,
    bronze_path,
    require_non_default_data_dir,
    utc_now,
)


def test_bronze_path() -> None:
    """Bronze paths are dataset-specific Parquet files."""
    assert bronze_path(Path("data"), "deputados") == Path("data/bronze/deputados.parquet")


def test_utc_now_normalizes_naive_datetimes() -> None:
    """Naive injected timestamps are treated as UTC."""
    naive = datetime(2024, 1, 2, 3, 4, 5)

    assert utc_now(naive) == naive.replace(tzinfo=UTC)


def test_utc_now_preserves_aware_datetimes() -> None:
    """Aware injected timestamps are converted to UTC."""
    aware = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))

    assert utc_now(aware) == aware.astimezone(UTC)


def test_require_non_default_data_dir_rejects_only_limited_default_runs(
    tmp_path: Path,
) -> None:
    """A limit option may not target the default data directory."""
    with pytest.raises(ValueError, match="limit options require a non-default --data-dir"):
        require_non_default_data_dir(Path("data"), limited=True)

    require_non_default_data_dir(Path("data"), limited=False)
    require_non_default_data_dir(tmp_path, limited=True)


def test_ingest_result_is_a_summary() -> None:
    """IngestResult carries dataset, rows, path, and timestamp."""
    now = datetime(2024, 1, 2, tzinfo=UTC)
    result = IngestResult("partidos", 3, Path("data/bronze/partidos.parquet"), now)

    assert result.dataset == "partidos"
    assert result.rows == 3
    assert result.ingested_at == now
