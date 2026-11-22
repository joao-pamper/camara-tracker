"""Dataset-specific ingestion modules."""

from camara_tracker.ingest.common import IngestResult
from camara_tracker.ingest.deputados import ingest_deputados
from camara_tracker.ingest.partidos import ingest_partidos

__all__ = ["IngestResult", "ingest_deputados", "ingest_partidos"]
