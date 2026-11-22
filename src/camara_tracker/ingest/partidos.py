"""Ingest parties into the bronze layer."""

from datetime import datetime

from pydantic import ValidationError

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.common import IngestResult, bronze_path, utc_now
from camara_tracker.models import Partido
from camara_tracker.storage import PARTIDOS_SCHEMA, records_to_table, write_parquet


def ingest_partidos(
    client: CamaraClient,
    settings: Settings,
    *,
    max_pages: int | None = None,
    now: datetime | None = None,
) -> IngestResult:
    """Fetch, validate, deduplicate, and store parties."""
    ingested_at = utc_now(now)
    by_id: dict[int, Partido] = {}

    for raw in client.iter_items(
        "partidos",
        {"idLegislatura": settings.legislatura},
        max_pages=max_pages,
    ):
        try:
            partido = Partido.model_validate(raw)
        except ValidationError as exc:
            row_id = raw.get("id") if isinstance(raw, dict) else None
            raise ValueError(f"Invalid partido row id={row_id}: {exc}") from exc
        by_id[partido.id] = partido

    rows = [by_id[key] for key in sorted(by_id)]
    path = bronze_path(settings.data_dir, "partidos")
    table = records_to_table(rows, PARTIDOS_SCHEMA, ingested_at, settings.legislatura)
    write_parquet(table, path)
    return IngestResult("partidos", len(rows), path, ingested_at)
