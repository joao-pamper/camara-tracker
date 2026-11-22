"""Parquet storage helpers."""

import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

DEPUTADOS_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("uri", pa.string()),
        pa.field("nome", pa.string()),
        pa.field("siglaPartido", pa.string()),
        pa.field("uriPartido", pa.string()),
        pa.field("siglaUf", pa.string()),
        pa.field("idLegislatura", pa.int32()),
        pa.field("urlFoto", pa.string()),
        pa.field("email", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_legislatura", pa.int32()),
    ]
)

PARTIDOS_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("sigla", pa.string()),
        pa.field("nome", pa.string()),
        pa.field("uri", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_legislatura", pa.int32()),
    ]
)

DESPESAS_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("ano", pa.int32()),
        pa.field("mes", pa.int32()),
        pa.field("tipoDespesa", pa.string()),
        pa.field("codDocumento", pa.string()),
        pa.field("tipoDocumento", pa.string()),
        pa.field("codTipoDocumento", pa.int32()),
        pa.field("dataDocumento", pa.string()),
        pa.field("numDocumento", pa.string()),
        pa.field("parcela", pa.int32()),
        pa.field("valorDocumento", pa.float64()),
        pa.field("valorLiquido", pa.float64()),
        pa.field("valorGlosa", pa.float64()),
        pa.field("nomeFornecedor", pa.string()),
        pa.field("cnpjCpfFornecedor", pa.string()),
        pa.field("codLote", pa.int64()),
        pa.field("urlDocumento", pa.string()),
        pa.field("numRessarcimento", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_legislatura", pa.int32()),
        pa.field("_deputado_id", pa.int64()),
        pa.field("_ano", pa.int32()),
    ]
)


def write_parquet(table: pa.Table, path: Path) -> None:
    """Atomically write a table with zstd compression."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        pq.write_table(table, temp_path, compression="zstd")
        os.replace(temp_path, path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise


def read_parquet(path: Path) -> pa.Table:
    """Read a Parquet table from disk."""
    return pq.read_table(path)


def records_to_table(
    records: Sequence[BaseModel],
    schema: pa.Schema,
    ingested_at: datetime,
    legislatura: int,
    constants: Mapping[str, Any] | None = None,
) -> pa.Table:
    """Build a table with explicit metadata columns from pydantic records."""
    normalized_at = (
        ingested_at.replace(tzinfo=UTC)
        if ingested_at.tzinfo is None
        else ingested_at.astimezone(tz=UTC)
    )
    static_values = dict(constants or {})
    extra_columns = {"_ingested_at", "_legislatura"}
    columns: dict[str, list[Any]] = {
        name: [getattr(record, name, None) for record in records]
        for name in schema.names
        if name not in extra_columns and name not in static_values
    }
    columns["_ingested_at"] = [normalized_at] * len(records)
    columns["_legislatura"] = [legislatura] * len(records)
    for name, value in static_values.items():
        if name in schema.names and name not in extra_columns:
            columns[name] = [value] * len(records)
    arrays = [pa.array(columns[name], type=schema.field(name).type) for name in schema.names]
    return pa.Table.from_arrays(arrays, schema=schema)
