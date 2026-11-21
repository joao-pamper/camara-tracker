"""Command-line interface for camara-tracker."""

import typer

from camara_tracker import __version__

app: typer.Typer = typer.Typer(no_args_is_help=True, add_completion=False)


@app.callback()
def main() -> None:
    """Commands for camara-tracker."""


@app.command(name="version")
def version() -> None:
    """Print the installed package version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
