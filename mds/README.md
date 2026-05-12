# mds — Modern Data Stack CLI

This is the management CLI for the Analytics Engineering in the Modern Data Stack course repo.
It is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) member installed into the
shared virtual environment as the `mds` command.

## Setup

From the repo root, install all workspace packages (including `mds`) with:

```bash
uv sync
```

Once you activate the virtual environment, you can invoke the CLI directly:

```bash
mds --help
```

Or without activating the venv:

```bash
uv run mds --help
```

## Commands

### `generate-source-data`
Generates synthetic call center source data (parquet files) used by the ingestion pipelines.

```bash
uv run mds generate-source-data
uv run mds generate-source-data --global-start-date 2025-01-01 --global-end-date 2025-03-31
```

### `init-env`
Initializes local environment files (`.env`, `profiles.yml`, warehouse startup SQL) with
absolute paths for your machine. Run this once after cloning the repo.

```bash
uv run mds init-env
uv run mds init-env --no-prompt   # skip overwrite confirmations
```

### `reset`
Resets state for one or more components. Useful when starting fresh or debugging.
Targets can be combined in a single command.

```bash
uv run mds reset dagster
uv run mds reset dlt
uv run mds reset warehouse
uv run mds reset source-data
uv run mds reset metabase
uv run mds reset dagster dlt warehouse   # combine targets
uv run mds reset all                     # reset everything
```

### `assignment`
Installs stub files for a course module so students can work through the TODOs,
or restores the answer key when done.

```bash
uv run mds assignment --module 5           # install stubs for module 5
uv run mds assignment --module 5 --restore # restore the answer key
uv run mds assignment --module 5 --no-reset  # skip the pipeline state reset prompt
uv run mds assignment --restore-all          # restore ALL modules at once (recover from broken state)
uv run mds assignment --restore-all --no-reset  # same, skip the pipeline reset prompt
```

Available modules: 5, 6, 8, 9

### `sync-answers`
(Instructor command) Copies the current live model files into each module's `answers/`
directory. Run this after editing any live model file to keep answer keys in sync.

```bash
uv run mds sync-answers
```

### `cleanup-dupes`
Removes duplicate parquet files injected by the Module 5 assignment setup.

```bash
uv run mds cleanup-dupes
```

## Package structure

```
mds/
├── pyproject.toml          ← package definition and [project.scripts] entry point
├── README.md               ← this file
└── src/
    └── mds/
        ├── __init__.py
        ├── cli.py              ← all CLI commands (argparse)
        └── data_generation/    ← call center simulation logic
            ├── constants.py
            ├── helpers.py
            ├── call_center_simulation.py
            └── tests/
```
