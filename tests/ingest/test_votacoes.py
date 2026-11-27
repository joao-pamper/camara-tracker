"""Tests for votações and votos ingestion and compaction."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pytest
import respx

from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest.votacoes import (
    compact_votacoes,
    ingest_votacoes,
    is_fresh,
    shard_paths,
    votacoes_shard_path,
    votos_shard_path,
)
from camara_tracker.models import Votacao, Voto
from camara_tracker.storage import (
    VOTACOES_SCHEMA,
    VOTOS_SCHEMA,
    read_parquet,
    records_to_table,
    write_parquet,
)

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
TODAY = date(2026, 1, 1)
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


def votacoes_payload(votacao_id: str) -> dict[str, Any]:
    """Build a minimal single-votação API response."""
    return {
        "dados": [{"id": votacao_id, "siglaOrgao": "PLEN", "aprovacao": 1}],
        "links": [],
    }


def make_settings(tmp_path: Path) -> Settings:
    """Build settings with no retry delays or request throttling."""
    return Settings(
        data_dir=tmp_path,
        items_per_page=2,
        min_interval_s=0.0,
        max_retries=0,
    )


def make_client(settings: Settings) -> CamaraClient:
    """Build a client with fake sleep and clock."""
    clock = FakeClock()
    return CamaraClient(settings, sleep=RecordingSleep(clock), clock=clock)


def write_shards(
    settings: Settings,
    ano: int,
    mes: int,
    ingested_at: datetime,
    votacoes: list[Votacao],
    votos: list[Voto],
    votacao_ids: list[str],
) -> tuple[Path, Path]:
    """Write both shards with the metadata used by ingestion."""
    votacoes_table = records_to_table(
        votacoes, VOTACOES_SCHEMA, ingested_at, settings.legislatura
    )
    votos_table = records_to_table(
        votos,
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
    for table in (votacoes_table, votos_table):
        metadata = dict(table.schema.metadata or {})
        metadata[b"_ingested_at"] = ingested_at.isoformat().encode("utf-8")
        table = table.replace_schema_metadata(metadata)
        if table.schema == VOTACOES_SCHEMA:
            write_parquet(table, votacoes_shard_path(settings, ano, mes))
        else:
            write_parquet(table, votos_shard_path(settings, ano, mes))
    return shard_paths(settings, ano, mes)


def test_shard_paths_match_layout(tmp_path: Path) -> None:
    """Shards are partitioned by year and two-digit month."""
    settings = make_settings(tmp_path)

    assert votacoes_shard_path(settings, 2024, 3) == (
        tmp_path / "bronze" / "votacoes" / "_shards" / "ano=2024" / "mes=03.parquet"
    )
    assert votos_shard_path(settings, 2024, 12) == (
        tmp_path / "bronze" / "votos" / "_shards" / "ano=2024" / "mes=12.parquet"
    )
    assert shard_paths(settings, 2024, 3) == (
        votacoes_shard_path(settings, 2024, 3),
        votos_shard_path(settings, 2024, 3),
    )


def test_is_fresh_requires_both_shards_and_checks_age(tmp_path: Path) -> None:
    """Completeness, closed months, and the open-month age gate drive freshness."""
    settings = make_settings(tmp_path)
    closed = write_shards(
        settings,
        2024,
        3,
        NOW - timedelta(days=365),
        [Votacao(id="v1")],
        [Voto(deputado_id=1)],
        ["v1"],
    )
    write_parquet(
        records_to_table([Votacao(id="v0")], VOTACOES_SCHEMA, NOW, settings.legislatura),
        votacoes_shard_path(settings, 2026, 1),
    )
    assert votacoes_shard_path(settings, 2026, 1).exists()
    assert not votos_shard_path(settings, 2026, 1).exists()
    open_fresh = write_shards(
        settings,
        2026,
        2,
        NOW - timedelta(hours=1),
        [Votacao(id="v2")],
        [Voto(deputado_id=2)],
        ["v2"],
    )
    open_stale = write_shards(
        settings,
        2026,
        3,
        NOW - timedelta(hours=25),
        [Votacao(id="v3")],
        [Voto(deputado_id=3)],
        ["v3"],
    )

    assert closed[0].exists() and closed[1].exists()
    assert is_fresh(2024, 3, TODAY, settings, NOW)
    assert not is_fresh(2026, 1, TODAY, settings, NOW)
    assert is_fresh(2026, 2, TODAY, settings, NOW)
    assert open_stale[0].exists() and open_stale[1].exists()
    assert not is_fresh(2026, 3, TODAY, settings, NOW)
    assert open_fresh[0].exists()


def test_ingest_fetches_only_orgao_votos_and_writes_both_shards(tmp_path: Path) -> None:
    """A month with two PLEN votes and one other organ stores all three votações."""
    settings = make_settings(tmp_path)
    progress: list[tuple[int, int]] = []

    with respx.mock(assert_all_called=False) as router:
        votacoes_route = router.get(f"{BASE_URL}/votacoes").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("votacoes_page_1.json")),
                httpx.Response(200, json=load_fixture("votacoes_page_2.json")),
            ]
        )
        nominal_route = router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_nominal.json"))
        )
        symbolic_route = router.get(f"{BASE_URL}/votacoes/2400003-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_empty.json"))
        )
        with make_client(settings) as client:
            result = ingest_votacoes(
                client,
                settings,
                years=[2024],
                months=[3],
                today=TODAY,
                now=NOW,
                progress=lambda done, total: progress.append((done, total)),
            )

    assert result.months_fetched == 1
    assert result.months_skipped == 0
    assert result.failed == []
    assert result.votacoes_rows == 3
    assert result.votos_rows == 2
    assert progress == [(1, 1)]
    assert votacoes_route.call_count == 2
    assert nominal_route.call_count == 1
    assert symbolic_route.call_count == 1
    assert "itens" not in nominal_route.calls[0].request.url.params

    votacoes_path, votos_path = shard_paths(settings, 2024, 3)
    votacoes_table = read_parquet(votacoes_path)
    votos_table = read_parquet(votos_path)
    assert votacoes_table.schema.equals(VOTACOES_SCHEMA, check_metadata=False)
    assert votos_table.schema.equals(VOTOS_SCHEMA, check_metadata=False)
    assert votacoes_table.column("id").to_pylist() == ["2400001-1", "2400002-1", "2400003-1"]
    assert votacoes_table.column("siglaOrgao").to_pylist() == ["PLEN", "CEDESNAT", "PLEN"]
    assert votos_table.column("_votacao_id").to_pylist() == ["2400001-1", "2400001-1"]
    assert votos_table.column("deputado_id").to_pylist() == [166402, 178957]
    assert votos_table.column("_ingested_at").to_pylist() == [NOW, NOW]


def test_second_run_skips_closed_month_and_force_refetches(tmp_path: Path) -> None:
    """A complete closed month is skipped; force still refetches it."""
    settings = make_settings(tmp_path)

    def mock_routes(router: respx.Router) -> None:
        router.get(f"{BASE_URL}/votacoes").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("votacoes_page_1.json")),
                httpx.Response(200, json=load_fixture("votacoes_page_2.json")),
            ]
        )
        router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_nominal.json"))
        )
        router.get(f"{BASE_URL}/votacoes/2400003-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_empty.json"))
        )

    with respx.mock(assert_all_called=False) as router:
        mock_routes(router)
        with make_client(settings) as client:
            first = ingest_votacoes(
                client, settings, years=[2024], months=[3], today=TODAY, now=NOW
            )

    assert first.months_fetched == 1

    with respx.mock(assert_all_called=False) as router:
        with make_client(settings) as client:
            second = ingest_votacoes(
                client, settings, years=[2024], months=[3], today=TODAY, now=NOW
            )
        assert len(router.calls) == 0

    assert second.months_fetched == 0
    assert second.months_skipped == 1

    with respx.mock(assert_all_called=False) as router:
        mock_routes(router)
        with make_client(settings) as client:
            forced = ingest_votacoes(
                client, settings, years=[2024], months=[3], force=True, today=TODAY, now=NOW
            )

    assert forced.months_fetched == 1
    assert forced.votacoes_rows == 3
    assert forced.votos_rows == 2


def test_votos_failure_leaves_no_shards_and_other_months_succeed(tmp_path: Path) -> None:
    """A failed votos request aborts one month without blocking the next."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/votacoes").mock(
            side_effect=[
                httpx.Response(200, json=votacoes_payload("2400101-1")),
                httpx.Response(200, json=votacoes_payload("2400201-1")),
            ]
        )
        router.get(f"{BASE_URL}/votacoes/2400101-1/votos").mock(
            return_value=httpx.Response(500)
        )
        router.get(f"{BASE_URL}/votacoes/2400201-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_nominal.json"))
        )
        with make_client(settings) as client:
            result = ingest_votacoes(
                client,
                settings,
                years=[2024],
                months=[1, 2],
                today=TODAY,
                now=NOW,
            )

    assert result.months_fetched == 1
    assert len(result.failed) == 1
    assert result.failed[0][:3] == (2024, 1, "2400101-1")
    assert "HTTP 500" in result.failed[0][3]
    assert result.votos_rows == 2
    for path in shard_paths(settings, 2024, 1):
        assert not path.exists()
    for path in shard_paths(settings, 2024, 2):
        assert path.exists()


def test_invalid_voto_row_reports_votacao_and_index(tmp_path: Path) -> None:
    """Invalid voto rows identify the votação and row index."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/votacoes").mock(
            return_value=httpx.Response(200, json=votacoes_payload("2400001-1"))
        )
        router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_invalid.json"))
        )
        with make_client(settings) as client:
            result = ingest_votacoes(
                client, settings, years=[2024], months=[3], today=TODAY, now=NOW
            )

    assert len(result.failed) == 1
    assert "votação 2400001-1 row 0" in result.failed[0][3]
    assert not votacoes_shard_path(settings, 2024, 3).exists()


def test_votos_404_is_recorded_and_does_not_fail_month(tmp_path: Path) -> None:
    """A 404 on one votação's votos is recorded while other votos are written."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/votacoes").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("votacoes_page_1.json")),
                httpx.Response(200, json=load_fixture("votacoes_page_2.json")),
            ]
        )
        router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(404, json={"status": 404})
        )
        router.get(f"{BASE_URL}/votacoes/2400003-1/votos").mock(
            return_value=httpx.Response(200, json=load_fixture("votos_nominal.json"))
        )
        with make_client(settings) as client:
            result = ingest_votacoes(
                client, settings, years=[2024], months=[3], today=TODAY, now=NOW
            )

    assert result.months_fetched == 1
    assert result.failed == []
    assert result.votacoes_rows == 3
    assert result.votos_rows == 2
    assert result.votos_not_found == [(2024, 3, "2400001-1")]
    votacoes_path, votos_path = shard_paths(settings, 2024, 3)
    assert votacoes_path.exists() and votos_path.exists()
    assert read_parquet(votacoes_path).column("id").to_pylist() == [
        "2400001-1",
        "2400002-1",
        "2400003-1",
    ]
    assert read_parquet(votos_path).column("_votacao_id").to_pylist() == [
        "2400003-1",
        "2400003-1",
    ]


def test_votos_500_still_fails_whole_month(tmp_path: Path) -> None:
    """A 500 on votos keeps the month-level failure behavior."""
    settings = make_settings(tmp_path)

    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/votacoes").mock(
            return_value=httpx.Response(200, json=votacoes_payload("2400001-1"))
        )
        router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(500)
        )
        with make_client(settings) as client:
            result = ingest_votacoes(
                client, settings, years=[2024], months=[3], today=TODAY, now=NOW
            )

    assert result.months_fetched == 0
    assert len(result.failed) == 1
    assert result.failed[0][:3] == (2024, 3, "2400001-1")
    assert result.votos_not_found == []
    for path in shard_paths(settings, 2024, 3):
        assert not path.exists()


def test_compact_merges_sorts_and_validates_schema(tmp_path: Path) -> None:
    """Compaction merges months into sorted yearly files for both datasets."""
    settings = make_settings(tmp_path)
    write_shards(
        settings,
        2024,
        1,
        NOW,
        [Votacao(id="b"), Votacao(id="a")],
        [Voto(deputado_id=9), Voto(deputado_id=2)],
        ["b", "b"],
    )
    write_shards(
        settings,
        2024,
        2,
        NOW,
        [Votacao(id="c")],
        [Voto(deputado_id=4), Voto(deputado_id=3)],
        ["c", "c"],
    )

    results = compact_votacoes(settings, years=[2024])

    assert [result.dataset for result in results] == ["votacoes", "votos"]
    votacoes_path = tmp_path / "bronze" / "votacoes" / "votacoes_2024.parquet"
    votos_path = tmp_path / "bronze" / "votos" / "votos_2024.parquet"
    assert [result.path for result in results] == [votacoes_path, votos_path]
    assert [result.rows for result in results] == [3, 4]

    votacoes_table = read_parquet(votacoes_path)
    votos_table = read_parquet(votos_path)
    assert votacoes_table.schema.equals(VOTACOES_SCHEMA, check_metadata=False)
    assert votos_table.schema.equals(VOTOS_SCHEMA, check_metadata=False)
    assert votacoes_table.column("id").to_pylist() == ["a", "b", "c"]
    assert votos_table.column("_votacao_id").to_pylist() == ["b", "b", "c", "c"]
    assert votos_table.column("deputado_id").to_pylist() == [2, 9, 3, 4]


def test_limit_votacoes_requires_non_default_data_dir() -> None:
    """A partial votações run refuses the default data directory."""
    settings = Settings(data_dir=Path("data"))

    with (
        make_client(settings) as client,
        pytest.raises(ValueError, match="limit options require a non-default --data-dir"),
    ):
        ingest_votacoes(client, settings, years=[2024], months=[3], limit_votacoes=2)


def test_compact_without_shards_fails_clearly(tmp_path: Path) -> None:
    """Compaction reports the missing year instead of writing empty files."""
    settings = make_settings(tmp_path)

    with pytest.raises(FileNotFoundError, match="No votações shards for ano 2024"):
        compact_votacoes(settings, years=[2024])
