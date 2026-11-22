"""Ingest deputies into the bronze layer."""

from datetime import datetime

from pydantic import ValidationError

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.common import IngestResult, bronze_path, utc_now
from camara_tracker.models import Deputado
from camara_tracker.storage import DEPUTADOS_SCHEMA, records_to_table, write_parquet


def ingest_deputados(
    client: CamaraClient,
    settings: Settings,
    *,
    max_pages: int | None = None,
    now: datetime | None = None,
) -> IngestResult:
    """Fetch, validate, deduplicate, and store deputies."""
    ingested_at = utc_now(now)
    by_id: dict[int, Deputado] = {}

    for raw in client.iter_items(
        "deputados",
        {"idLegislatura": settings.legislatura},
        max_pages=max_pages,
    ):
        try:
            deputado = Deputado.model_validate(raw)
        except ValidationError as exc:
            row_id = raw.get("id") if isinstance(raw, dict) else None
            raise ValueError(f"Invalid deputado row id={row_id}: {exc}") from exc
        by_id[deputado.id] = deputado

    rows = [by_id[key] for key in sorted(by_id)]
    path = bronze_path(settings.data_dir, "deputados")
    table = records_to_table(rows, DEPUTADOS_SCHEMA, ingested_at, settings.legislatura)
    write_parquet(table, path)
    return IngestResult("deputados", len(rows), path, ingested_at)
