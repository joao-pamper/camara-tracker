"""Shared helpers for dataset ingestion."""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class IngestResult:
    """Summary of one completed ingestion run."""

    dataset: str
    rows: int
    path: Path
    ingested_at: datetime


def bronze_path(data_dir: Path, dataset: str) -> Path:
    """Build the bronze-layer Parquet path for a dataset."""
    return data_dir / "bronze" / f"{dataset}.parquet"


def require_non_default_data_dir(data_dir: Path, *, limited: bool) -> None:
    """Reject partial runs that would write into the default data directory."""
    if limited and data_dir.resolve() == Path("data").resolve():
        raise ValueError("limit options require a non-default --data-dir")


def utc_now(now: datetime | None = None) -> datetime:
    """Return an injected timestamp normalized to UTC, or the current time."""
    if now is None:
        return datetime.now(tz=UTC)
    if now.tzinfo is None:
        return now.replace(tzinfo=UTC)
    return now.astimezone(tz=UTC)
