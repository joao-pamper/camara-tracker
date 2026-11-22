"""Tests for ingest CLI commands."""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from typer.testing import CliRunner

from camara_tracker.cli import app
from camara_tracker.storage import read_parquet

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    """Load a small API response fixture."""
    path = FIXTURES_DIR / name
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


@pytest.mark.parametrize(
    ("dataset", "fixture"),
    [
        ("deputados", "deputados_page_1.json"),
        ("partidos", "partidos_page_1.json"),
    ],
)
def test_ingest_command_writes_parquet(
    tmp_path: Path,
    dataset: str,
    fixture: str,
) -> None:
    """Each ingest command exits successfully and writes its Parquet file."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/{dataset}").mock(
            return_value=httpx.Response(200, json=load_fixture(fixture))
        )
        result = CliRunner().invoke(
            app,
            [
                "ingest",
                dataset,
                "--legislatura",
                "57",
                "--data-dir",
                str(tmp_path),
                "--max-pages",
                "1",
            ],
        )

    path = tmp_path / "bronze" / f"{dataset}.parquet"
    assert result.exit_code == 0, result.stdout
    assert "rows" in result.stdout
    assert path.exists()
    assert read_parquet(path).num_rows == 2


def test_ingest_despesas_command_writes_shards(tmp_path: Path) -> None:
    """The despesas command writes a shard and reports its summary."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_single_page.json"))
        )
        result = CliRunner().invoke(
            app,
            [
                "ingest",
                "despesas",
                "--year",
                "2024",
                "--deputy-id",
                "1",
                "--data-dir",
                str(tmp_path),
            ],
        )

    path = tmp_path / "bronze" / "despesas" / "_shards" / "ano=2024" / "deputado=1.parquet"
    assert result.exit_code == 0, result.stdout
    assert "fetched 1" in result.stdout
    assert "rows 1" in result.stdout
    assert path.exists()
    assert read_parquet(path).num_rows == 1


def test_ingest_despesas_command_exits_nonzero_on_failure(tmp_path: Path) -> None:
    """Invalid API rows make the despesas command exit non-zero."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_invalid.json"))
        )
        result = CliRunner().invoke(
            app,
            [
                "ingest",
                "despesas",
                "--year",
                "2024",
                "--deputy-id",
                "1",
                "--data-dir",
                str(tmp_path),
            ],
        )

    assert result.exit_code == 1
    assert "failed 1" in result.stdout


def test_despesas_compact_command_reports_totals(tmp_path: Path) -> None:
    """The compact command merges shards and reports rows and liquid total."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/deputados/1/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_single_page.json"))
        )
        router.get(f"{BASE_URL}/deputados/2/despesas").mock(
            return_value=httpx.Response(200, json=load_fixture("despesas_single_page.json"))
        )
        runner = CliRunner()
        ingest_result = runner.invoke(
            app,
            [
                "ingest",
                "despesas",
                "--year",
                "2024",
                "--deputy-id",
                "1",
                "--deputy-id",
                "2",
                "--data-dir",
                str(tmp_path),
            ],
        )
        compact_result = runner.invoke(
            app,
            [
                "ingest",
                "despesas-compact",
                "--year",
                "2024",
                "--data-dir",
                str(tmp_path),
            ],
        )

    path = tmp_path / "bronze" / "despesas" / "despesas_2024.parquet"
    assert ingest_result.exit_code == 0, ingest_result.stdout
    assert compact_result.exit_code == 0, compact_result.stdout
    assert "2024: 2 rows, valorLiquido -207.60" in compact_result.stdout
    assert path.exists()


def test_despesas_compact_command_without_shards_exits_nonzero(tmp_path: Path) -> None:
    """The compact command fails clearly when a year has no shards."""
    result = CliRunner().invoke(
        app,
        [
            "ingest",
            "despesas-compact",
            "--year",
            "2024",
            "--data-dir",
            str(tmp_path),
        ],
    )

    output = result.stdout + result.stderr
    assert result.exit_code == 1
    assert "No despesas shards for ano 2024" in output


def mock_votacoes_routes(router: respx.Router) -> None:
    """Mock one month of votações with a nominal and a symbolic PLEN vote."""
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


def test_ingest_votacoes_command_writes_shards(tmp_path: Path) -> None:
    """The votacoes command reports progress and writes both monthly shards."""
    with respx.mock(assert_all_called=False) as router:
        mock_votacoes_routes(router)
        result = CliRunner().invoke(
            app,
            ["ingest", "votacoes", "--year", "2024", "--month", "3", "--data-dir", str(tmp_path)],
        )

    assert result.exit_code == 0, result.stdout
    assert "votacoes: 1/1 months" in result.stdout
    assert "votacoes rows 3" in result.stdout
    assert "votos rows 2" in result.stdout
    assert (
        tmp_path / "bronze" / "votacoes" / "_shards" / "ano=2024" / "mes=03.parquet"
    ).exists()
    assert (
        tmp_path / "bronze" / "votos" / "_shards" / "ano=2024" / "mes=03.parquet"
    ).exists()


def test_ingest_votacoes_command_exits_nonzero_on_failure(tmp_path: Path) -> None:
    """A failed votos request makes the votacoes command exit non-zero."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE_URL}/votacoes").mock(
            return_value=httpx.Response(
                200,
                json={"dados": [{"id": "2400001-1", "siglaOrgao": "PLEN"}], "links": []},
            )
        )
        router.get(f"{BASE_URL}/votacoes/2400001-1/votos").mock(
            return_value=httpx.Response(500)
        )
        result = CliRunner().invoke(
            app,
            ["ingest", "votacoes", "--year", "2024", "--month", "3", "--data-dir", str(tmp_path)],
        )

    assert result.exit_code == 1
    assert "failed 1" in result.stdout


def test_ingest_votacoes_command_reports_404_votos_as_warning(tmp_path: Path) -> None:
    """A 404 on votos exits 0, writes shards, and prints summary/verbose lines."""
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
        result = CliRunner().invoke(
            app,
            ["ingest", "votacoes", "--year", "2024", "--month", "3", "--data-dir", str(tmp_path)],
        )

    assert result.exit_code == 0, result.stdout
    assert "votos not found (404): 1 votações" in result.stdout
    assert "2400001-1" not in result.stdout
    assert (
        tmp_path / "bronze" / "votacoes" / "_shards" / "ano=2024" / "mes=03.parquet"
    ).exists()
    assert (
        tmp_path / "bronze" / "votos" / "_shards" / "ano=2024" / "mes=03.parquet"
    ).exists()

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
        verbose_result = CliRunner().invoke(
            app,
            [
                "ingest",
                "votacoes",
                "--year",
                "2024",
                "--month",
                "3",
                "--force",
                "--verbose",
                "--data-dir",
                str(tmp_path),
            ],
        )

    assert verbose_result.exit_code == 0, verbose_result.stdout
    assert "votos not found (404): 1 votações" in verbose_result.stdout
    assert "2024-03 2400001-1" in verbose_result.stdout


def test_votacoes_compact_command_reports_rows(tmp_path: Path) -> None:
    """The compact command reports rows for both yearly datasets."""
    with respx.mock(assert_all_called=False) as router:
        mock_votacoes_routes(router)
        runner = CliRunner()
        ingest_result = runner.invoke(
            app,
            ["ingest", "votacoes", "--year", "2024", "--month", "3", "--data-dir", str(tmp_path)],
        )
        compact_result = runner.invoke(
            app,
            ["ingest", "votacoes-compact", "--year", "2024", "--data-dir", str(tmp_path)],
        )

    assert ingest_result.exit_code == 0, ingest_result.stdout
    assert compact_result.exit_code == 0, compact_result.stdout
    assert "2024: votacoes 3 rows" in compact_result.stdout
    assert "2024: votos 2 rows" in compact_result.stdout
