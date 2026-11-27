"""Tests for Parquet storage helpers."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from camara_tracker import storage
from camara_tracker.models import Deputado, DeputadoHistorico, Despesa, Proposicao, Votacao, Voto
from camara_tracker.storage import (
    DEPUTADOS_HISTORICO_SCHEMA,
    DEPUTADOS_SCHEMA,
    DESPESAS_SCHEMA,
    PARTIDOS_SCHEMA,
    PROPOSICOES_SCHEMA,
    VOTACOES_SCHEMA,
    VOTOS_SCHEMA,
    read_parquet,
    records_to_table,
    write_parquet,
)

NOW = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def make_deputados_table() -> pa.Table:
    """Build a small deputies table for storage tests."""
    records = [
        Deputado(id=2, nome="Beto", email=None),
        Deputado(id=1, nome="Ada", siglaUf="SP"),
    ]
    return records_to_table(records, DEPUTADOS_SCHEMA, NOW, 57)


def test_write_read_roundtrip(tmp_path: Path) -> None:
    """Writing and reading preserves schema and values."""
    path = tmp_path / "bronze" / "deputados.parquet"
    table = make_deputados_table()

    write_parquet(table, path)
    result = read_parquet(path)

    assert result.schema == table.schema
    assert result.to_pylist() == table.to_pylist()
    assert result.column("_ingested_at").to_pylist() == [NOW, NOW]
    assert result.column("_legislatura").to_pylist() == [57, 57]


def test_write_parquet_uses_zstd(tmp_path: Path) -> None:
    """Parquet files use zstd compression."""
    path = tmp_path / "bronze" / "partidos.parquet"
    table = records_to_table([], PARTIDOS_SCHEMA, NOW, 57)

    write_parquet(table, path)
    metadata = pq.ParquetFile(path).metadata

    assert metadata is not None
    assert metadata.row_group(0).column(0).compression == "ZSTD"


def test_write_parquet_is_atomic_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed write leaves no temp file and keeps the previous target."""
    path = tmp_path / "bronze" / "deputados.parquet"
    table = make_deputados_table()
    write_parquet(table, path)
    original = path.read_bytes()

    def fail(table: pa.Table, where: Path, **_: Any) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(storage.pq, "write_table", fail)

    with pytest.raises(RuntimeError, match="boom"):
        write_parquet(table, path)

    assert path.read_bytes() == original
    assert list(path.parent.glob("*.tmp")) == []


def test_records_to_table_empty_has_schema(tmp_path: Path) -> None:
    """No records still produces a table with the explicit schema."""
    assert tmp_path.exists()
    table = records_to_table([], PARTIDOS_SCHEMA, NOW, 57)

    assert table.num_rows == 0
    assert table.schema == PARTIDOS_SCHEMA


def test_records_to_table_adds_constant_columns() -> None:
    """Despesa records receive shard constants with the explicit schema."""
    table = records_to_table(
        [Despesa(ano=2024, valorLiquido=-103.8)],
        DESPESAS_SCHEMA,
        NOW,
        57,
        constants={"_deputado_id": 62881, "_ano": 2024},
    )

    assert table.schema == DESPESAS_SCHEMA
    assert table.column("_deputado_id").to_pylist() == [62881]
    assert table.column("_ano").to_pylist() == [2024]
    assert table.column("valorLiquido").to_pylist() == [-103.8]


def test_historico_and_proposicoes_schemas_are_explicit() -> None:
    """New bronze schemas carry metadata and shard constants."""
    historico = records_to_table(
        [DeputadoHistorico(id=62881, dataHora="2023-02-01T00:00")],
        DEPUTADOS_HISTORICO_SCHEMA,
        NOW,
        57,
    )
    proposicao = records_to_table(
        [Proposicao(id=2418080, ano=2024)],
        PROPOSICOES_SCHEMA,
        NOW,
        57,
        constants={"_deputado_id": 62881, "_ano": 2024},
    )

    assert historico.schema.equals(DEPUTADOS_HISTORICO_SCHEMA, check_metadata=False)
    assert historico.schema.field("_ingested_at").type == pa.timestamp("us", tz="UTC")
    assert historico.schema.field("_legislatura").type == pa.int32()
    assert historico.column("dataHora").to_pylist() == ["2023-02-01T00:00"]
    assert proposicao.schema.equals(PROPOSICOES_SCHEMA, check_metadata=False)
    assert proposicao.schema.field("_deputado_id").type == pa.int64()
    assert proposicao.schema.field("_ano").type == pa.int32()
    assert proposicao.column("_deputado_id").to_pylist() == [62881]
    assert proposicao.column("ano").to_pylist() == [2024]


def test_records_to_table_votacoes_and_votos() -> None:
    """Vote tables carry explicit schemas and the per-row votação id."""
    votacao = Votacao(id="2400001-1", siglaOrgao="PLEN", aprovacao=1)
    voto = Voto(
        deputado_id=166402,
        deputado_siglaPartido="PRD",
        deputado_siglaUf="GO",
        tipoVoto="Sim",
        dataRegistroVoto="2024-03-12T17:56:06",
    )
    votacoes_table = records_to_table([votacao], VOTACOES_SCHEMA, NOW, 57)
    votos_table = records_to_table(
        [voto],
        VOTOS_SCHEMA,
        NOW,
        57,
        constants={"_votacao_id": "2400001-1"},
    )

    assert votacoes_table.schema == VOTACOES_SCHEMA
    assert votacoes_table.column("id").to_pylist() == ["2400001-1"]
    assert votos_table.schema == VOTOS_SCHEMA
    assert votos_table.column("_votacao_id").to_pylist() == ["2400001-1"]
    assert votos_table.column("deputado_id").to_pylist() == [166402]
