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
