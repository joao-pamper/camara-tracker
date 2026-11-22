# camara-tracker
Ingests Brazilian Chamber of Deputies open data (https://dadosabertos.camara.leg.br/api/v2, no auth) into Parquet files; analytics later run in Databricks (Free Edition, uploads via Volume).

## Stack
Python 3.11+, httpx, pydantic v2, pyarrow, typer. Tests: pytest + respx. Lint/type: ruff + mypy strict. macOS dev, venv in .venv.

## Layout
src/camara_tracker/api (HTTP client), ingest (one module per dataset), models.py (pydantic), storage.py (Parquet IO), cli.py. Tests mirror src in tests/. Data goes in data/ (gitignored). Databricks code goes in databricks/.

## Rules
- Type hints on everything. Small pure functions. No global state. No print; use logging.
- Never hit the live API in tests; mock with respx and use small JSON fixtures in tests/fixtures/.
- Every new function/module ships with tests in the same change. Run `make lint && make test` before finishing and fix failures.
- API responses look like {"dados": [...], "links": [...]}; paginate with `pagina` and `itens`; follow the "next" link. Verify field names against a real response (curl) or https://dadosabertos.camara.leg.br/swagger/api.html. Never guess field names.
- Validate API data with pydantic models; store Parquet with explicit schemas; keep raw API field names in bronze, rename only in later layers.
- Ingestion must be idempotent and resumable. Scope is the current legislature (idLegislatura=57) unless told otherwise.
- Add dependencies only when needed, and add them to pyproject.toml.
- Do not ask clarifying questions; choose a sensible default and state it in one line.
- Final reply: at most 5 lines (what changed, test results, assumptions).
- Prefer reading only the files you need; do not scan the whole repo.
- Bronze despesas keeps raw API values. The sign of valorLiquido is unreliable (negative on most rows); never use it for totals. Expense metrics use the rule defined in the metrics phase.