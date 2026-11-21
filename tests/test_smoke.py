"""Smoke tests for package import and CLI entrypoint."""

from typer.testing import CliRunner

import camara_tracker
from camara_tracker.cli import app


def test_package_imports() -> None:
    """The package exposes a version string."""
    assert isinstance(camara_tracker.__version__, str)
    assert camara_tracker.__version__


def test_version_command() -> None:
    """The CLI version command exits successfully and prints the version."""
    result = CliRunner().invoke(app, ["version"])

    assert result.exit_code == 0
    assert camara_tracker.__version__ in result.stdout
