"""Command-line interface for camara-tracker."""

from pathlib import Path
from typing import Annotated

import typer

from camara_tracker import __version__
from camara_tracker.api import CamaraClient
from camara_tracker.config import Settings
from camara_tracker.ingest import ingest_deputados, ingest_partidos

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


if __name__ == "__main__":
    app()
