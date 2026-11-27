"""Ingest propositions authored by deputies into the bronze layer."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
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
from camara_tracker.models import Proposicao
from camara_tracker.storage import PROPOSICOES_SCHEMA, records_to_table, write_parquet

INGESTED_AT_KEY = b"_ingested_at"


@dataclass(frozen=True)
class ProposicoesResult:
    """Summary of a sharded propositions ingestion run."""

    fetched: int
    skipped: int
    failed: list[tuple[int, int, str]]
    rows: int


def shard_path(settings: Settings, ano: int, deputado_id: int) -> Path:
    """Build the proposition shard path for one deputy and year."""
    return (
        settings.data_dir
        / "bronze"
        / "proposicoes"
        / "_shards"
        / f"ano={ano}"
        / f"deputado={deputado_id}.parquet"
    )


def _with_ingested_metadata(table: pa.Table, ingested_at: datetime) -> pa.Table:
    metadata: dict[bytes, bytes] = dict(table.schema.metadata or {})
    metadata[INGESTED_AT_KEY] = ingested_at.isoformat().encode("utf-8")
    return table.replace_schema_metadata(metadata)


def _read_ingested_at(path: Path) -> datetime | None:
    try:
        metadata = pq.ParquetFile(path).schema_arrow.metadata or {}
        raw = metadata.get(INGESTED_AT_KEY)
        if raw is not None:
            parsed = datetime.fromisoformat(raw.decode("utf-8"))
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        table = pq.read_table(path, columns=["_ingested_at"])
    except (pa.ArrowInvalid, OSError, ValueError, KeyError):
        return None
    if table.num_rows == 0:
        return None
    value: Any = table.column("_ingested_at")[0].as_py()
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def is_fresh(
    path: Path,
    ano: int,
    today: date,
    settings: Settings,
    now: datetime,
) -> bool:
    """Return whether a shard is complete and recent enough to skip."""
    if not path.exists():
        return False
    if ano < today.year:
        return True
    ingested_at = _read_ingested_at(path)
    if ingested_at is None:
        return False
    return utc_now(now) - ingested_at <= timedelta(hours=settings.shard_max_age_hours)


def _deputy_ids(
    settings: Settings,
    deputy_ids: Sequence[int] | None,
    limit_deputies: int | None,
) -> list[int]:
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


def _years(settings: Settings, years: Sequence[int] | None, today: date) -> list[int]:
    if years is not None:
        return sorted(set(years))
    return list(range(settings.first_year, today.year + 1))


def ingest_proposicoes(
    client: CamaraClient,
    settings: Settings,
    *,
    years: Sequence[int] | None = None,
    deputy_ids: Sequence[int] | None = None,
    limit_deputies: int | None = None,
    force: bool = False,
    today: date | None = None,
    now: datetime | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> ProposicoesResult:
    """Fetch proposition shards, isolating failures per deputy-year."""
    require_non_default_data_dir(settings.data_dir, limited=limit_deputies is not None)
    today_value = today or date.today()
    ingested_at = utc_now(now)
    deputy_values = _deputy_ids(settings, deputy_ids, limit_deputies)
    year_values = _years(settings, years, today_value)
    total = len(deputy_values) * len(year_values)
    fetched = 0
    skipped = 0
    rows = 0
    failed: list[tuple[int, int, str]] = []
    done = 0

    for deputado_id in deputy_values:
        for ano in year_values:
            path = shard_path(settings, ano, deputado_id)
            if not force and is_fresh(path, ano, today_value, settings, ingested_at):
                skipped += 1
                done += 1
                if progress is not None:
                    progress(done, total)
                continue

            try:
                raw_rows = list(
                    client.iter_items(
                        "/proposicoes",
                        {"idDeputadoAutor": deputado_id, "ano": ano},
                    )
                )
                records: list[Proposicao] = []
                for index, raw in enumerate(raw_rows):
                    try:
                        records.append(Proposicao.model_validate(raw))
                    except ValidationError as exc:
                        raise ValueError(
                            f"Invalid proposição for deputado {deputado_id} ano {ano} "
                            f"row {index}: {exc}"
                        ) from exc
                table = records_to_table(
                    records,
                    PROPOSICOES_SCHEMA,
                    ingested_at,
                    settings.legislatura,
                    constants={"_deputado_id": deputado_id, "_ano": ano},
                )
                write_parquet(_with_ingested_metadata(table, ingested_at), path)
            except Exception as exc:
                failed.append((deputado_id, ano, str(exc)))
            else:
                fetched += 1
                rows += len(records)

            done += 1
            if progress is not None:
                progress(done, total)

    return ProposicoesResult(fetched=fetched, skipped=skipped, failed=failed, rows=rows)


def compact_proposicoes(
    settings: Settings,
    *,
    years: Sequence[int] | None = None,
) -> list[IngestResult]:
    """Merge propositions shards into one sorted bronze file per year.

    Each row means "deputy appears as an author", not "first signer": the API
    filter ``idDeputadoAutor`` has no signer-order field, so a proposition with
    several authors correctly appears once per author and is not deduplicated.
    """
    year_values = _years(settings, years, date.today())
    results: list[IngestResult] = []

    for ano in year_values:
        shard_dir = settings.data_dir / "bronze" / "proposicoes" / "_shards" / f"ano={ano}"
        paths = sorted(shard_dir.glob("deputado=*.parquet"))
        if not paths:
            raise FileNotFoundError(
                f"No proposicoes shards for ano {ano}; "
                f"run `camara-tracker ingest proposicoes --year {ano}` first"
            )

        tables = [pq.read_table(path) for path in paths]
        merged = pa.concat_tables(tables, promote_options="default").cast(PROPOSICOES_SCHEMA)
        merged = merged.sort_by(
            [
                ("_deputado_id", "ascending", "at_end"),
                ("id", "ascending", "at_end"),
            ]
        )
        path = settings.data_dir / "bronze" / "proposicoes" / f"proposicoes_{ano}.parquet"
        ingested_at = utc_now(None)
        write_parquet(merged, path)
        results.append(IngestResult("proposicoes", merged.num_rows, path, ingested_at))

    return results
