"""Ingest plenary votações and their votes into the bronze layer."""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from camara_tracker.api import ApiError, CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.common import IngestResult, utc_now
from camara_tracker.models import Votacao, Voto
from camara_tracker.storage import (
    VOTACOES_SCHEMA,
    VOTOS_SCHEMA,
    records_to_table,
    write_parquet,
)

logger = logging.getLogger(__name__)

INGESTED_AT_KEY = b"_ingested_at"


@dataclass(frozen=True)
class VotacoesResult:
    """Summary of a sharded votações ingestion run."""

    months_fetched: int
    months_skipped: int
    failed: list[tuple[int, int, str, str]]
    votacoes_rows: int
    votos_rows: int
    votos_not_found: list[tuple[int, int, str]] = field(default_factory=list)


def votacoes_shard_path(settings: Settings, ano: int, mes: int) -> Path:
    """Build the votações shard path for one month."""
    return (
        settings.data_dir
        / "bronze"
        / "votacoes"
        / "_shards"
        / f"ano={ano}"
        / f"mes={mes:02d}.parquet"
    )


def votos_shard_path(settings: Settings, ano: int, mes: int) -> Path:
    """Build the votos shard path for one month."""
    return (
        settings.data_dir
        / "bronze"
        / "votos"
        / "_shards"
        / f"ano={ano}"
        / f"mes={mes:02d}.parquet"
    )


def shard_paths(settings: Settings, ano: int, mes: int) -> tuple[Path, Path]:
    """Return the votações and votos shard paths for one month."""
    return votacoes_shard_path(settings, ano, mes), votos_shard_path(settings, ano, mes)


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
    ano: int,
    mes: int,
    today: date,
    settings: Settings,
    now: datetime,
) -> bool:
    """Return whether both shards exist and the closed/open month can be skipped."""
    votacoes_path, votos_path = shard_paths(settings, ano, mes)
    if not (votacoes_path.exists() and votos_path.exists()):
        return False
    if (ano, mes) < (today.year, today.month):
        return True
    ingested_at = _read_ingested_at(votacoes_path)
    if ingested_at is None:
        return False
    return utc_now(now) - ingested_at <= timedelta(
        hours=settings.open_month_max_age_hours
    )


def _months(
    settings: Settings,
    years: Sequence[int] | None,
    months: Sequence[int] | None,
    today: date,
) -> list[tuple[int, int]]:
    current = (today.year, today.month)
    if years is None and months is None:
        result: list[tuple[int, int]] = []
        ano, mes = settings.first_year, 1
        while (ano, mes) <= current:
            result.append((ano, mes))
            mes += 1
            if mes > 12:
                ano, mes = ano + 1, 1
        return result
    year_values = (
        sorted(set(years))
        if years is not None
        else list(range(settings.first_year, today.year + 1))
    )
    month_values = sorted(set(months)) if months is not None else list(range(1, 13))
    return sorted(
        (ano, mes)
        for ano in year_values
        for mes in month_values
        if 1 <= mes <= 12 and (ano, mes) <= current
    )


def _compact_years(settings: Settings, years: Sequence[int] | None) -> list[int]:
    if years is not None:
        return sorted(set(years))
    root = settings.data_dir / "bronze" / "votacoes" / "_shards"
    return sorted(
        int(path.name.removeprefix("ano="))
        for path in root.glob("ano=*")
        if path.is_dir() and path.name.removeprefix("ano=").isdigit()
    )


def ingest_votacoes(
    client: CamaraClient,
    settings: Settings,
    *,
    years: Sequence[int] | None = None,
    months: Sequence[int] | None = None,
    orgao: str | None = None,
    limit_votacoes: int | None = None,
    force: bool = False,
    today: date | None = None,
    now: datetime | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> VotacoesResult:
    """Fetch month shards, keeping both shards atomic per month."""
    today_value = today or date.today()
    ingested_at = utc_now(now)
    orgao_value = settings.votes_orgao if orgao is None else orgao
    month_values = _months(settings, years, months, today_value)
    total = len(month_values)
    months_fetched = 0
    months_skipped = 0
    votacoes_rows = 0
    votos_rows = 0
    failed: list[tuple[int, int, str, str]] = []
    votos_not_found: list[tuple[int, int, str]] = []

    for done, (ano, mes) in enumerate(month_values, start=1):
        votacoes_path, votos_path = shard_paths(settings, ano, mes)
        if not force and is_fresh(ano, mes, today_value, settings, ingested_at):
            months_skipped += 1
            if progress is not None:
                progress(done, total)
            continue

        current_votacao_id = ""
        try:
            params = {
                "dataInicio": date(ano, mes, 1).isoformat(),
                "dataFim": date(ano, mes, _last_day(ano, mes)).isoformat(),
                "ordenarPor": "dataHoraRegistro",
                "ordem": "ASC",
            }
            raw_votacoes = list(client.iter_items("/votacoes", params))
            deduped: dict[str, dict[str, Any]] = {}
            for raw in raw_votacoes:
                key = str(raw.get("id"))
                if key not in deduped:
                    deduped[key] = raw
            selected = list(deduped.values())
            if limit_votacoes is not None:
                selected = selected[:limit_votacoes]

            votacao_records: list[Votacao] = []
            for index, raw in enumerate(selected):
                normalized = dict(raw)
                normalized["id"] = str(raw.get("id"))
                try:
                    votacao_records.append(Votacao.model_validate(normalized))
                except ValidationError as exc:
                    raise ValueError(
                        f"Invalid votação {normalized['id']} row {index}: {exc}"
                    ) from exc

            voto_records: list[Voto] = []
            votacao_ids: list[str] = []
            for votacao in votacao_records:
                if votacao.siglaOrgao != orgao_value:
                    continue
                current_votacao_id = str(votacao.id)
                try:
                    raw_votos = list(
                        client.iter_items(f"/votacoes/{votacao.id}/votos", paginate=False)
                    )
                except ApiError as exc:
                    if exc.status_code == 404:
                        logger.warning(
                            "votos not found (404): %s-%02d votação %s",
                            ano,
                            mes,
                            votacao.id,
                        )
                        votos_not_found.append((ano, mes, str(votacao.id)))
                        continue
                    raise
                for index, raw in enumerate(raw_votos):
                    try:
                        voto_records.append(Voto.model_validate(raw))
                    except ValidationError as exc:
                        raise ValueError(
                            f"Invalid voto for votação {votacao.id} row {index}: {exc}"
                        ) from exc
                    votacao_ids.append(current_votacao_id)
        except Exception as exc:
            failed.append((ano, mes, current_votacao_id, str(exc)))
            if progress is not None:
                progress(done, total)
            continue

        votos_table = records_to_table(
            voto_records,
            VOTOS_SCHEMA,
            ingested_at,
            settings.legislatura,
            constants={"_votacao_id": ""},
        )
        votos_table = votos_table.set_column(
            votos_table.schema.get_field_index("_votacao_id"),
            "_votacao_id",
            pa.array(votacao_ids, type=pa.string()),
        )
        votacoes_table = records_to_table(
            votacao_records, VOTACOES_SCHEMA, ingested_at, settings.legislatura
        )
        write_parquet(_with_ingested_metadata(votos_table, ingested_at), votos_path)
        write_parquet(_with_ingested_metadata(votacoes_table, ingested_at), votacoes_path)
        months_fetched += 1
        votacoes_rows += len(votacao_records)
        votos_rows += len(voto_records)
        if progress is not None:
            progress(done, total)

    return VotacoesResult(
        months_fetched=months_fetched,
        months_skipped=months_skipped,
        failed=failed,
        votacoes_rows=votacoes_rows,
        votos_rows=votos_rows,
        votos_not_found=votos_not_found,
    )


def _last_day(ano: int, mes: int) -> int:
    if mes == 12:
        return 31
    return (date(ano, mes + 1, 1) - timedelta(days=1)).day


def compact_votacoes(
    settings: Settings,
    *,
    years: Sequence[int] | None = None,
) -> list[IngestResult]:
    """Merge month shards into one sorted file per dataset and year."""
    results: list[IngestResult] = []
    for ano in _compact_years(settings, years):
        votacoes_dir = settings.data_dir / "bronze" / "votacoes" / "_shards" / f"ano={ano}"
        votos_dir = settings.data_dir / "bronze" / "votos" / "_shards" / f"ano={ano}"
        votacoes_paths = sorted(votacoes_dir.glob("mes=*.parquet"))
        votos_paths = sorted(votos_dir.glob("mes=*.parquet"))
        if not votacoes_paths:
            raise FileNotFoundError(
                f"No votações shards for ano {ano}; "
                f"run `camara-tracker ingest votacoes --year {ano}` first"
            )
        if not votos_paths:
            raise FileNotFoundError(
                f"No votos shards for ano {ano}; "
                f"run `camara-tracker ingest votacoes --year {ano}` first"
            )

        votacoes_table = (
            pa.concat_tables(
                [pq.read_table(path) for path in votacoes_paths], promote_options="default"
            )
            .cast(VOTACOES_SCHEMA)
            .sort_by([("id", "ascending", "at_end")])
        )
        votos_table = (
            pa.concat_tables(
                [pq.read_table(path) for path in votos_paths], promote_options="default"
            )
            .cast(VOTOS_SCHEMA)
            .sort_by(
                [
                    ("_votacao_id", "ascending", "at_end"),
                    ("deputado_id", "ascending", "at_end"),
                ]
            )
        )
        ingested_at = utc_now(None)
        votacoes_path = (
            settings.data_dir / "bronze" / "votacoes" / f"votacoes_{ano}.parquet"
        )
        votos_path = settings.data_dir / "bronze" / "votos" / f"votos_{ano}.parquet"
        write_parquet(votacoes_table, votacoes_path)
        write_parquet(votos_table, votos_path)
        results.append(
            IngestResult("votacoes", votacoes_table.num_rows, votacoes_path, ingested_at)
        )
        results.append(
            IngestResult("votos", votos_table.num_rows, votos_path, ingested_at)
        )
    return results
