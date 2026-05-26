# AGENTS.md — Monorepo Root

This file provides guidance for AI coding agents (GitHub Copilot, Claude, etc.) working in this monorepo.

## Repository Overview

This is a full-stack analytics engineering monorepo for a call center data platform. It combines data ingestion, transformation, orchestration, and visualization:

```
.
├── analytics_system/   # Dagster orchestration & ingestion pipelines (separate venv)
├── call_center/        # dbt transformation project
├── mds/                # `mds` CLI package (uv workspace member)
├── data/               # Raw parquet source data + DuckDB warehouse files
├── data_vis_metabase/  # Metabase BI tool (Docker)
├── assignments/        # Course module stubs and answer keys
└── scripts/            # Helper SQL/shell scripts
```

## Virtual Environments — Critical Rule

**There are two separate virtual environments. Always use the right one.**

| Work area | venv location | How to run |
|-----------|--------------|------------|
| Root (dbt, mds CLI, data generation, linting) | `.venv/` at repo root | `uv run <cmd>` from repo root |
| Dagster orchestration | `analytics_system/.venv/` | `cd analytics_system && uv run <cmd>` |

**Never run Dagster commands from the repo root** and **never run `mds` commands from inside `analytics_system/`**.

## Setup (first time)

```bash
# 1. Install root venv (dbt, mds, data generation, linting)
uv sync                          # run from repo root

# 2. Install Dagster venv
cd analytics_system && uv sync   # run from analytics_system/

# 3. Install dbt packages
cd ..   # back to repo root
uv run dbt deps --project-dir call_center --profiles-dir call_center

# 4. Generate synthetic source data
uv run mds generate-source-data

# 5. Initialize environment files (.env, profiles.yml, warehouse SQL)
uv run mds init-env --no-prompt
```

## The `mds` CLI

`mds` is the primary management CLI for this repo. **Always run it from the repo root** using `uv run mds`.

```bash
# Generate source data
uv run mds generate-source-data

# Initialize .env and profiles.yml with absolute paths for your machine
uv run mds init-env --no-prompt   # always use --no-prompt; bare init-env will block on interactive prompts

# Reset state (combine targets freely)
uv run mds reset dagster
uv run mds reset dlt
uv run mds reset warehouse
uv run mds reset metabase
uv run mds reset dagster dlt warehouse   # multiple targets
uv run mds reset all

# Course assignments
uv run mds assignment --module 5           # install stubs
uv run mds assignment --module 5 --restore # restore answer key
uv run mds assignment --restore-all        # recover all modules

# Instructor only
uv run mds sync-answers
uv run mds cleanup-dupes
```

## dbt (call_center project)

The dbt project lives in `call_center/`. dbt commands must be run from the **repo root** using `uv run`, because `dbt` is installed in the root venv.

```bash
# Run from repo root, targeting the call_center/ directory
# NOTE: incremental models require --vars with start_date and end_date
uv run dbt run --project-dir call_center --profiles-dir call_center --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
uv run dbt test --project-dir call_center --profiles-dir call_center --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
uv run dbt build --project-dir call_center --profiles-dir call_center --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
uv run dbt compile --project-dir call_center --profiles-dir call_center --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'

# Full rebuild from scratch (no vars needed)
uv run dbt build --project-dir call_center --profiles-dir call_center --full-refresh

# Specific model selector
uv run dbt run --project-dir call_center --profiles-dir call_center --select staging --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
uv run dbt run --project-dir call_center --profiles-dir call_center --select +mart_surveys --vars '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
```

See [`call_center/AGENTS.md`](call_center/AGENTS.md) for dbt-specific guidance.

## Dagster (analytics_system)

Dagster commands must be run from **inside `analytics_system/`** using its own venv.

```bash
cd analytics_system

# Start the Dagster UI
uv run dg dev

# Launch assets from the CLI
uv run dg launch --assets dlt_filesystem_calls_source_calls --partition-range 2025-01-01...2025-01-05
uv run dg launch --assets dlt_filesystem_surveys_source_surveys --partition 2025-02-01
```

See [`analytics_system/AGENTS.md`](analytics_system/AGENTS.md) for Dagster-specific guidance.

## Linting

Both modules use `ruff`. Run from the repo root for everything except Dagster code:

```bash
# Lint + autofix root project (includes mds/, call_center models are SQL — not linted by ruff)
uv run ruff check . --fix
uv run ruff format .

# Lint analytics_system separately (uses its own venv)
cd analytics_system && uv run ruff check . --fix
cd analytics_system && uv run ruff format .
```

## Tests

```bash
# All tests must be run from the repo root
uv run pytest
```

## Exploring the Warehouse

```bash
# Open DuckDB CLI with all databases loaded (omit -ui — that opens a browser and will hang an agent)
duckdb -init scripts/warehouse-startup.sql
```

## Metabase (BI Visualization)

```bash
cd data_vis_metabase
docker compose up -d
# Open http://localhost in browser
# Use DuckDB path: /warehouse/warehouse_dev.duckdb
```

## Key Files for Agents

| File | Purpose |
|------|---------|
| `pyproject.toml` | Root project dependencies (Python 3.13.5, dbt-duckdb, mds workspace) |
| `analytics_system/pyproject.toml` | Dagster project dependencies (separate venv) |
| `call_center/dbt_project.yml` | dbt project config, schema mappings |
| `call_center/profiles.yml` | dbt connection profile (generated by `mds init-env`) |
| `analytics_system/.env` | Dagster home path (generated by `mds init-env`) |
| `mds/README.md` | Full `mds` CLI documentation |
