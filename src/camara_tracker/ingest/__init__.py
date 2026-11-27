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
from camara_tracker.ingest.historico import ingest_historico
from camara_tracker.ingest.partidos import ingest_partidos
from camara_tracker.ingest.proposicoes import (
    ProposicoesResult,
    compact_proposicoes,
    ingest_proposicoes,
)
from camara_tracker.ingest.proposicoes import is_fresh as is_proposicoes_fresh
from camara_tracker.ingest.proposicoes import shard_path as proposicoes_shard_path
from camara_tracker.ingest.votacoes import (
    VotacoesResult,
    compact_votacoes,
    ingest_votacoes,
    shard_paths,
    votacoes_shard_path,
    votos_shard_path,
)
from camara_tracker.ingest.votacoes import is_fresh as is_votacoes_fresh

__all__ = [
    "DespesasResult",
    "IngestResult",
    "ProposicoesResult",
    "VotacoesResult",
    "compact_despesas",
    "compact_proposicoes",
    "compact_votacoes",
    "ingest_deputados",
    "ingest_despesas",
    "ingest_historico",
    "ingest_partidos",
    "ingest_proposicoes",
    "ingest_votacoes",
    "is_fresh",
    "is_proposicoes_fresh",
    "is_votacoes_fresh",
    "proposicoes_shard_path",
    "shard_path",
    "shard_paths",
    "votacoes_shard_path",
    "votos_shard_path",
]
