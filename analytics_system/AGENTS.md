# AGENTS.md — analytics_system (Dagster)

This module is the Dagster orchestration layer. It has its **own isolated virtual environment** that is separate from the repo root.

## Virtual Environment

This directory has its own `pyproject.toml` and `.venv/`. Always use `uv run` from **within this directory** — do not run Dagster commands from the repo root.

```bash
# Install dependencies (first time or after pyproject.toml changes)
cd analytics_system
uv sync
```

Python version: **3.13.5** (matches repo root).

## Running Dagster

```bash
cd analytics_system

# Start the Dagster UI (webserver + daemon)
uv run dg dev
# Opens at http://localhost:3000
```

## Launching Assets / Pipelines via CLI

```bash
cd analytics_system

# Launch with a partition range
uv run dg launch --assets dlt_filesystem_calls_source_calls --partition-range 2025-01-01...2025-01-05

# Launch a single partition
uv run dg launch --assets dlt_filesystem_surveys_source_surveys --partition 2025-02-01
```

## Environment Setup

Dagster requires a `.env` file to persist run state (asset materializations, logs). This is **not** checked in.

```bash
# Generate from repo root (uses mds CLI in root venv)
cd ..
uv run mds init-env --no-prompt   # always use --no-prompt; bare init-env will block on interactive prompts
```

The `.env` file sets `DAGSTER_HOME` to an absolute path on your machine. See `.env.example` for the template.

## Project Structure

```
analytics_system/
├── pyproject.toml              # Dagster dependencies, dg project config
├── .venv/                      # Isolated Dagster virtual environment
├── .env                        # Local environment (DAGSTER_HOME) — not committed
├── .env.example                # Template for .env
└── src/
    └── analytics_system/
        ├── definitions.py      # Top-level Dagster Definitions object
        ├── constants.py        # Shared constants (paths, partition config)
        └── defs/
            ├── dbt_assets/     # Dagster-dbt asset definitions
            └── filesystem_duckdb_ingest/  # dlt-based ingestion assets
```

## Key Dependencies

| Package | Role |
|---------|------|
| `dagster` | Orchestration framework |
| `dagster-dbt` | Dagster ↔ dbt integration (wraps `call_center/` project) |
| `dagster-dlt` | Dagster ↔ dlt integration for ingestion pipelines |
| `dlt[duckdb]` | Data load tool, ingests parquet → DuckDB |
| `dbt-duckdb` | dbt adapter for DuckDB |
| `dagster-webserver` | (dev) UI server |
| `dagster-dg-cli` | (dev) `dg` CLI for launching assets |

## Linting

```bash
cd analytics_system
uv run ruff check . --fix
uv run ruff format .
```

Line length: 120. Config in `pyproject.toml` `[tool.ruff]`.

## Tests

```bash
# Tests must be run from the repo root, not from within analytics_system/
cd .. && uv run pytest
```

## Important Notes for Agents

- **Do not** `uv run` Dagster commands from the repo root — the root venv does not include Dagster.
- The `call_center/` dbt project is referenced by the Dagster assets via a relative path (`../call_center`). Don't move either directory.
- Partition keys are dates (e.g., `2025-01-01`). Ranges use `...` notation: `2025-01-01...2025-01-31`.
- `DAGSTER_HOME` in `.env` must be an absolute path; relative paths will break run history persistence.

## DuckDB Concurrency: Race Condition History & Deferred Hardening

`call_center/profiles.yml` attaches all three ingest DuckDB files
(`ingest_calls.duckdb`, `ingest_crm.duckdb`, `ingest_surveys.duckdb`)
read-only on **every** dbt invocation (`dbt_seeds` and `dbt_analytics`).
DuckDB uses a single-writer file lock, so if a dlt asset is actively writing
to one of those files at the same moment a dbt asset attaches it, one side
errors out.

A previous bug in `CustomDagsterDbtTranslator.get_asset_spec`
(`defs/dbt_assets/assets.py`) wired the seed build's upstream dlt asset keys
incorrectly (all three pointed at the `calls_source` prefix), so the crm/
surveys dlt ingestion assets were never actually linked as dependencies.
This let "materialize all" run the seed build concurrently with in-progress
crm/surveys dlt writes, causing DuckDB lock errors. That has been fixed —
seed models now correctly depend on all three dlt ingestion assets, so
Dagster's dependency-respecting execution order prevents the overlap for a
single "materialize all"/backfill selection.

**Known residual risk (accepted, not currently addressed):** dependency
edges only order asset runs *within* a single graph selection. They do not
protect against two independently-triggered runs overlapping (e.g. manually
re-running one dlt partition while an unrelated dbt run is already in
flight), nor against a future dependency edge being added incorrectly again.

If DuckDB lock races resurface outside the "materialize all" flow, consider
one of these previously-scoped options instead of re-litigating from
scratch:
- **Per-file mutual exclusion locks**: use `filelock` (a direct dependency —
  see the dlt state race section below) with one lock file per ingest
  DuckDB path. Each dlt ingestion asset acquires only its own file's lock
  (preserving dlt-to-dlt parallelism); both dbt asset defs acquire all
  three ingest-file locks, in a fixed sorted order, before invoking
  `dbt.cli(...)`.
- **Staging-copy redesign**: add an explicit step that copies raw tables
  from the ingest DuckDB files into `warehouse_dev.duckdb` once dlt sources
  are materialized, so downstream dbt models never attach the live ingest
  files directly. More work (touches `sources.yml`, model refs, and
  `profiles.yml`), but removes the cross-cutting `attach:` contention
  entirely and is the most parallel-friendly long-term option.

## Windows dlt `state.json` Race Condition (fixed)

Two Windows students hit an intermittent failure even after the "fixing dlt
issues" / Windows-compatibility fixes had been merged:

```
PermissionError: [WinError 5] Access is denied:
'...\.dlt\filesystem_crm_source\<tmp>' -> '...\.dlt\filesystem_crm_source\state.json'
```

**Root cause:** in `defs/filesystem_duckdb_ingest/loads.py`, all three
`dlt.pipeline(...)` objects are constructed eagerly as `@dlt_assets`
decorator arguments — i.e. at *module import time*, not lazily inside the
asset function body. `dlt.Pipeline.__init__` unconditionally saves
`.dlt/<pipeline_name>/state.json` on construction (`managed_state()` always
calls `_save_state`, even if nothing changed), and that save's underlying
`os.replace(tmp_path, dest_path)` is **not atomic on Windows** (dlt's own
source comment confirms this; it is atomic on POSIX). Dagster reimports
this module independently in multiple OS processes — one per step under the
default `multiprocess_executor`, plus `dg dev`'s webserver and daemon
processes on startup/workspace reload — so two processes can end up
constructing the *same-named* pipeline at nearly the same instant, racing
on the same `state.json` and intermittently raising `WinError 5` on
whichever process loses the rename race. This is genuinely Windows-specific
(not a per-student environment issue): confirmed via the vendored `dlt`
source that `FileStorage.save_atomic` has no retry/lock logic at all, and
confirmed unchanged between our pinned `dlt==1.26.0` and the latest release
(`1.30.0`) at the time of investigation — no upstream fix to pick up via a
version bump.

**Fix:** `loads.py` now wraps each `dlt.pipeline(...)` construction in a
`_locked_pipeline()` helper that acquires a `filelock.FileLock` keyed by
`pipeline_name` (lock files live in `.dlt/`, already git-ignored) before
constructing the pipeline object, with a 30s timeout that raises a clear
`TimeoutError` instead of hanging indefinitely if something ever holds the
lock too long. `filelock` was promoted from a transitive to an explicit
direct dependency in `pyproject.toml` since first-party code now imports it.

**Why this doesn't cost real parallelism:** the lock only guards the brief
moment of constructing/attaching to a pipeline object (a small JSON
read/write, independent of dataset size) — not the actual
extract/normalize/load work, which happens later inside the asset function
body via `dlt_resource.run(...)`, fully outside the lock. Each of the three
pipelines (`filesystem_calls_source`, `filesystem_crm_source`,
`filesystem_surveys_source`) has its own separate lock file, so they still
construct and run fully in parallel with each other; the lock only
serializes two processes that would otherwise race on the exact same
pipeline's state file.

**If this resurfaces or a similar race appears elsewhere:** don't reach for
a dlt version bump — the relevant `dlt` code (`FileStorage.save_atomic` in
`dlt/common/storages/file_storage.py`) has no Windows-aware handling at all,
and it's confirmed still true as of the review at the time of this fix.
`dlt` does already have a proper multi-reader/single-writer cross-process
lock utility (`TransactionalFile` in
`dlt/common/storages/transactional_file.py`) that would be the "correct"
place to fix this upstream, but as of that review it wasn't wired into
`Pipeline._save_state` anywhere in dlt's own codebase (only referenced from
its own test file) — worth checking if a future `dlt` upgrade adopts it.
