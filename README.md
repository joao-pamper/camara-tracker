# camara-tracker
Ingests Brazilian Chamber of Deputies Open Data API data into Parquet files for Databricks analytics.

Python 3.11+; run `make install` to create `.venv` and install the CLI plus dev tools.

- `make test`: run pytest with coverage.
- `make lint`: run Ruff and strict mypy.
- `make format`: format with Ruff and apply safe fixes.
- `camara-tracker version`: print the installed version.
Data is written to gitignored `data/`.
