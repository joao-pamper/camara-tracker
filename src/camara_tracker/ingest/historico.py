"""Ingest deputy status history into the bronze layer."""

from collections.abc import Callable, Sequence
from datetime import datetime

import pyarrow.parquet as pq
from pydantic import ValidationError

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.common import (
    IngestResult,
    bronze_path,
    require_non_default_data_dir,
    utc_now,
)
from camara_tracker.models import DeputadoHistorico
from camara_tracker.storage import DEPUTADOS_HISTORICO_SCHEMA, records_to_table, write_parquet


def _deputy_ids(
    settings: Settings,
    deputy_ids: Sequence[int] | None,
    limit_deputies: int | None,
) -> list[int]:
    """Read deputy ids from the bronze deputies file or use the explicit ids."""
    if deputy_ids is None:
        path = bronze_path(settings.data_dir, "deputados")
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found; run `camara-tracker ingest deputados` first"
            )
        table = pq.read_table(path, columns=["id"])
        ids = [int(value) for value in table.column("id").to_pylist() if value is not None]
    else:
        ids = [int(value) for value in deputy_ids]
    ids = sorted(set(ids))
    if limit_deputies is not None:
        ids = ids[:limit_deputies]
    return ids


def ingest_historico(
    client: CamaraClient,
    settings: Settings,
    *,
    deputy_ids: Sequence[int] | None = None,
    limit_deputies: int | None = None,
    now: datetime | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> IngestResult:
    """Fetch all status-history rows and write one sorted bronze file atomically."""
    require_non_default_data_dir(settings.data_dir, limited=limit_deputies is not None)
    ingested_at = utc_now(now)
    deputy_values = _deputy_ids(settings, deputy_ids, limit_deputies)
    total = len(deputy_values)
    records: list[DeputadoHistorico] = []
    failed: list[tuple[int, str]] = []

    for done, deputado_id in enumerate(deputy_values, start=1):
        try:
            raw_rows = list(
                client.iter_items(f"/deputados/{deputado_id}/historico", paginate=False)
            )
            for index, raw in enumerate(raw_rows):
                try:
                    records.append(DeputadoHistorico.model_validate(raw))
                except ValidationError as exc:
                    raise ValueError(
                        f"Invalid histórico for deputado {deputado_id} row {index}: {exc}"
                    ) from exc
        except Exception as exc:
            failed.append((deputado_id, str(exc)))
        if progress is not None:
            progress(done, total)

    if failed:
        details = ", ".join(f"{deputado_id}: {message}" for deputado_id, message in failed)
        raise ValueError(f"historico failed for {len(failed)} deputies: {details}")

    table = records_to_table(
        records, DEPUTADOS_HISTORICO_SCHEMA, ingested_at, settings.legislatura
    )
    table = table.sort_by(
        [
            ("id", "ascending", "at_end"),
            ("dataHora", "ascending", "at_end"),
        ]
    )
    path = bronze_path(settings.data_dir, "deputados_historico")
    write_parquet(table, path)
    return IngestResult("deputados_historico", table.num_rows, path, ingested_at)
