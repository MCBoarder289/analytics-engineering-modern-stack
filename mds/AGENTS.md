# AGENTS.md — mds (CLI Package)

`mds` is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) member installed into the **repo root venv**. It provides the primary management CLI for this monorepo.

## Running Commands

```bash
# From repo root (no activation needed)
uv run mds --help
uv run mds <command> [options]
```

## All Commands

```bash
# Generate synthetic source parquet data
uv run mds generate-source-data
uv run mds generate-source-data --global-start-date 2025-01-01 --global-end-date 2025-03-31

# Initialize .env, profiles.yml, and warehouse-startup.sql with absolute paths
uv run mds init-env --no-prompt      # always use --no-prompt; bare init-env will block on interactive prompts

# Reset component state
uv run mds reset dagster
uv run mds reset dlt
uv run mds reset warehouse
uv run mds reset source-data
uv run mds reset metabase
uv run mds reset dagster dlt warehouse   # combine targets
uv run mds reset all

# Course assignment management
uv run mds assignment --module 5              # install stubs for module 5
uv run mds assignment --module 5 --restore    # restore answer key
uv run mds assignment --module 5 --no-reset   # skip pipeline state reset prompt
uv run mds assignment --restore-all           # restore ALL modules
uv run mds assignment --restore-all --no-reset

# Sync live model files → answer key directories (instructor)
uv run mds sync-answers

# Remove duplicate parquet files injected by Module 5 assignment
uv run mds cleanup-dupes
```

Available assignment modules: **5, 6, 8, 9**

## Package Structure

```
mds/
├── pyproject.toml          # Package definition, entry point: mds = "mds.cli:main"
├── README.md
└── src/
    └── mds/
        ├── __init__.py
        ├── cli.py              # All CLI commands (argparse)
        └── data_generation/    # Call center simulation logic
            ├── constants.py
            ├── helpers.py
            ├── call_center_simulation.py
            └── tests/
```

## Development

`mds` is installed as an editable workspace package — changes to source files under `src/mds/` take effect immediately without reinstalling.

```bash
# Lint
uv run ruff check mds/ --fix
uv run ruff format mds/

# Run from repo root (not from within mds/)
uv run pytest mds/
```

## Important Notes for Agents

- `mds` is only available in the **root venv** — it is not installed in the `analytics_system/` Dagster venv.
- All `mds` commands must be run from the **repo root**.
- `mds init-env` writes files with **absolute paths** specific to the current machine. Never commit the generated `profiles.yml` or `.env` files.
