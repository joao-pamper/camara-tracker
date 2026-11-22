"""Dataset-specific ingestion modules."""

from camara_tracker.ingest.common import IngestResult
from camara_tracker.ingest.deputados import ingest_deputados
from camara_tracker.ingest.despesas import (
    DespesasResult,
    compact_despesas,
    ingest_despesas,
    is_fresh,
    shard_path,
)
from camara_tracker.ingest.partidos import ingest_partidos

__all__ = [
    "DespesasResult",
    "IngestResult",
    "compact_despesas",
    "ingest_deputados",
    "ingest_despesas",
    "ingest_partidos",
    "is_fresh",
    "shard_path",
]
