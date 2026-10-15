#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

uv sync --locked --group dev
uv pip check --python "${UV_PROJECT_ENVIRONMENT:-.venv}/bin/python"
uv run --locked --no-sync ruff check .
uv run --locked --no-sync python scripts/check_project.py
uv run --locked --no-sync pytest -q
