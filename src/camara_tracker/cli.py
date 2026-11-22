"""Command-line interface for camara-tracker."""

from pathlib import Path
from typing import Annotated

import typer

from camara_tracker import __version__
from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import (
    compact_despesas,
    ingest_deputados,
    ingest_despesas,
    ingest_partidos,
)
from camara_tracker.storage import read_parquet

DEFAULT_LEGISLATURA: int = Settings().legislatura
DEFAULT_DATA_DIR: Path = Settings().data_dir

app: typer.Typer = typer.Typer(no_args_is_help=True, add_completion=False)
ingest_app: typer.Typer = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(ingest_app, name="ingest")


@app.callback()
def main() -> None:
    """Commands for camara-tracker."""


@app.command(name="version")
def version() -> None:
    """Print the installed package version."""
    typer.echo(__version__)


@ingest_app.callback()
def ingest_main() -> None:
    """Ingest datasets into bronze Parquet files."""


def _report_despesas_progress(done: int, total: int) -> None:
    """Print shard progress every 25 deputy-years and at the end."""
    if done % 25 == 0 or done == total:
        typer.echo(f"despesas: {done}/{total} deputy-years")


@ingest_app.command(name="deputados")
def deputados(
    legislatura: Annotated[int, typer.Option("--legislatura")] = DEFAULT_LEGISLATURA,
    data_dir: Annotated[Path, typer.Option("--data-dir")] = DEFAULT_DATA_DIR,
    max_pages: Annotated[int | None, typer.Option("--max-pages")] = None,
) -> None:
    """Ingest deputies into the bronze layer."""
    settings = Settings(legislatura=legislatura, data_dir=data_dir)
    with CamaraClient() as client:
        result = ingest_deputados(client, settings, max_pages=max_pages)
    typer.echo(f"{result.dataset}: {result.rows} rows -> {result.path}")


@ingest_app.command(name="partidos")
def partidos(
    legislatura: Annotated[int, typer.Option("--legislatura")] = DEFAULT_LEGISLATURA,
    data_dir: Annotated[Path, typer.Option("--data-dir")] = DEFAULT_DATA_DIR,
    max_pages: Annotated[int | None, typer.Option("--max-pages")] = None,
) -> None:
    """Ingest parties into the bronze layer."""
    settings = Settings(legislatura=legislatura, data_dir=data_dir)
    with CamaraClient() as client:
        result = ingest_partidos(client, settings, max_pages=max_pages)
    typer.echo(f"{result.dataset}: {result.rows} rows -> {result.path}")


@ingest_app.command(name="despesas")
def despesas(
    year: Annotated[list[int] | None, typer.Option("--year")] = None,
    deputy_id: Annotated[list[int] | None, typer.Option("--deputy-id")] = None,
    limit_deputies: Annotated[int | None, typer.Option("--limit-deputies")] = None,
    force: Annotated[bool, typer.Option("--force")] = False,
    data_dir: Annotated[Path, typer.Option("--data-dir")] = DEFAULT_DATA_DIR,
) -> None:
    """Inspect deputy expenses into per-deputy-year bronze shards."""
    settings = Settings(data_dir=data_dir)
    try:
        with CamaraClient() as client:
            result = ingest_despesas(
                client,
                settings,
                years=year,
                deputy_ids=deputy_id,
                limit_deputies=limit_deputies,
                force=force,
                progress=_report_despesas_progress,
            )
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    for deputado_id, ano, message in result.failed:
        typer.echo(
            f"despesas failed: deputado {deputado_id} ano {ano}: {message}",
            err=True,
        )
    typer.echo(
        f"despesas: fetched {result.fetched}, skipped {result.skipped}, "
        f"failed {len(result.failed)}, rows {result.rows}"
    )
    if result.failed:
        raise typer.Exit(code=1)


@ingest_app.command(name="despesas-compact")
def despesas_compact(
    year: Annotated[list[int] | None, typer.Option("--year")] = None,
    data_dir: Annotated[Path, typer.Option("--data-dir")] = DEFAULT_DATA_DIR,
) -> None:
    """Merge expense shards into one bronze Parquet file per year."""
    settings = Settings(data_dir=data_dir)
    try:
        results = compact_despesas(settings, years=year)
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    for result in results:
        table = read_parquet(result.path)
        values = table.column("valorLiquido").to_pylist()
        total = sum(float(value) if value is not None else 0.0 for value in values)
        ano = result.path.stem.rsplit("_", 1)[-1]
        typer.echo(f"{ano}: {result.rows} rows, valorLiquido {total:.2f}")


if __name__ == "__main__":
    app()
