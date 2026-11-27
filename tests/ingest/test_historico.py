"""Tests for deputy status-history ingestion."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.historico import ingest_historico
from camara_tracker.models import Deputado
from camara_tracker.storage import (
    DEPUTADOS_HISTORICO_SCHEMA,
    DEPUTADOS_SCHEMA,
    read_parquet,
    records_to_table,
    write_parquet,
)

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


class FakeClock:
    """Deterministic monotonic clock advanced by fake sleeps."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSleep:
    """Sleep replacement that records delays and advances a fake clock."""

    def __init__(self, clock: FakeClock) -> None:
        self.calls: list[float] = []
        self._clock = clock

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self._clock.advance(seconds)


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    payload: dict[str, Any] = json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))
    return payload


def make_settings(tmp_path: Path) -> Settings:
    """Build settings with no retry delays or request throttling."""
    return Settings(data_dir=tmp_path, min_interval_s=0.0, max_retries=0)


def make_client(settings: Settings) -> CamaraClient:
    """Build a client with fake sleep and clock."""
    clock = FakeClock()
    return CamaraClient(settings, sleep=RecordingSleep(clock), clock=clock)


def write_deputados(settings: Settings, ids: list[int]) -> Path:
    """Write a bronze deputies file with the given ids."""
    path = settings.data_dir / "bronze" / "deputados.parquet"
    table = records_to_table(
        [Deputado(id=deputado_id) for deputado_id in ids],
        DEPUTADOS_SCHEMA,
        NOW,
        settings.legislatura,
    )
    write_parquet(table, path)
    return path


def test_ingest_historico_sorts_all_rows_into_one_file(tmp_path: Path) -> None:
    """Rows from all deputies are sorted by id and dataHora in one file."""
    settings = make_settings(tmp_path)
    write_deputados(settings, [2, 1])
    progress: list[tuple[int, int]] = []

    with respx.mock(assert_all_called=False) as router:
        route_1 = router.get(f"{BASE_URL}/deputados/1/historico").mock(
            return_value=httpx.Response(200, json=load_fixture("historico_page_1.json"))
        )
        route_2 = router.get(f"{BASE_URL}/deputados/2/historico").mock(
            return_value=httpx.Response(200, json=load_fixture("historico_page_2.json"))
        )
        with make_client(settings) as client:
            result = ingest_historico(
                client,
                settings,
                now=NOW,
                progress=lambda done, total: progress.append((done, total)),
            )

    path = tmp_path / "bronze" / "deputados_historico.parquet"
    table = read_parquet(path)
    assert result.dataset == "deputados_historico"
    assert result.path == path
    assert result.rows == 4
    assert result.ingested_at == NOW
    assert table.schema.equals(DEPUTADOS_HISTORICO_SCHEMA, check_metadata=False)
    assert table.column("id").to_pylist() == [1, 1, 1, 2]
    assert table.column("dataHora").to_pylist() == [
        "2023-02-01T00:00",
        "2024-06-01T00:00",
        "2025-01-01T00:00",
        "2024-02-01T00:00",
    ]
    assert table.column("_ingested_at").to_pylist() == [NOW] * 4
    assert table.column("_legislatura").to_pylist() == [57] * 4
    assert progress == [(1, 2), (2, 2)]
    assert "itens" not in route_1.calls[0].request.url.params
    assert route_2.call_count == 1


def test_limit_deputies_reads_only_the_first_ids(tmp_path: Path) -> None:
    """The limit option narrows the deputy list without touching the default dir."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        route = router.get(f"{BASE_URL}/deputados/1/historico").mock(
            return_value=httpx.Response(200, json=load_fixture("historico_page_1.json"))
        )
        with make_client(settings) as client:
            result = ingest_historico(
                client, settings, deputy_ids=[2, 1], limit_deputies=1, now=NOW
            )

    assert route.call_count == 1
    assert result.rows == 3


def test_failure_writes_nothing_and_lists_failures(tmp_path: Path) -> None:
    """One failed deputy aborts the run and leaves no bronze file."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/historico").mock(
            return_value=httpx.Response(200, json=load_fixture("historico_page_1.json"))
        )
        router.get(f"{BASE_URL}/deputados/2/historico").mock(
            return_value=httpx.Response(500)
        )
        with make_client(settings) as client, pytest.raises(ValueError) as exc_info:
            ingest_historico(client, settings, deputy_ids=[1, 2], now=NOW)

    message = str(exc_info.value)
    assert "historico failed for 1 deputies" in message
    assert "2:" in message
    assert "HTTP 500" in message
    assert not (tmp_path / "bronze" / "deputados_historico.parquet").exists()


def test_invalid_row_is_reported_with_deputy_and_index(tmp_path: Path) -> None:
    """Invalid API rows identify the deputy and row index."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/3/historico").mock(
            return_value=httpx.Response(200, json=load_fixture("historico_invalid.json"))
        )
        with make_client(settings) as client, pytest.raises(ValueError) as exc_info:
            ingest_historico(client, settings, deputy_ids=[3], now=NOW)

    assert "Invalid histórico for deputado 3 row 0" in str(exc_info.value)
    assert not (tmp_path / "bronze" / "deputados_historico.parquet").exists()


def test_missing_deputados_file_has_actionable_error(tmp_path: Path) -> None:
    """A missing deputies file tells the user which command to run."""
    settings = make_settings(tmp_path)

    with (
        make_client(settings) as client,
        pytest.raises(FileNotFoundError, match="ingest deputados"),
    ):
        ingest_historico(client, settings, now=NOW)


def test_limit_deputies_requires_non_default_data_dir() -> None:
    """A partial histórico run refuses the default data directory."""
    settings = Settings(data_dir=Path("data"))

    with (
        make_client(settings) as client,
        pytest.raises(ValueError, match="limit options require a non-default --data-dir"),
    ):
        ingest_historico(client, settings, deputy_ids=[1], limit_deputies=1)
