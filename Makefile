PYTHON ?= python3
VENV ?= .venv

.PHONY: install test lint format

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip
	$(VENV)/bin/python -m pip install -e ".[dev]"

test:
	$(VENV)/bin/python -m pytest

lint:
	$(VENV)/bin/python -m ruff check .
	$(VENV)/bin/python -m mypy

format:
	$(VENV)/bin/python -m ruff format .
	$(VENV)/bin/python -m ruff check --fix .
