
import argparse
import datetime
import shutil
from pathlib import Path

import duckdb

from data_generation.call_center_simulation import main as run_simulation

BASE_DIR = Path(__file__).parent.resolve()
DAGSTER_HOME = BASE_DIR / "analytics_system" / ".dagster_home"
DLT_DIR = BASE_DIR / ".dlt"
WAREHOUSE_DIR = BASE_DIR / "data" / "warehouse"
ENV_EXAMPLE = BASE_DIR / "analytics_system" / ".env.example"
ENV_FILE = BASE_DIR / "analytics_system" / ".env"

INGEST_CALLS_WAREHOUSE = WAREHOUSE_DIR / "ingest_calls.duckdb"
INGEST_CRM_WAREHOUSE = WAREHOUSE_DIR / "ingest_crm.duckdb"
INGEST_SURVEYS_WAREHOUSE = WAREHOUSE_DIR / "ingest_surveys.duckdb"

PROFILES_YAML_EXAMPLE = BASE_DIR / "call_center" / "profiles.yml.example"
PROFILES_YAML_FILE = BASE_DIR / "call_center" / "profiles.yml"

DEV_WAREHOUSE_PATH = WAREHOUSE_DIR / "warehouse_dev.duckdb"
PROD_WAREHOUSE_PATH = WAREHOUSE_DIR / "warehouse_prod.duckdb"

METABASE_DATA_PATH = BASE_DIR / "data_vis_metabase" / "pgdata"

DATA_DIR = BASE_DIR / "data"

def init_env(no_prompt=False):
    if not ENV_EXAMPLE.exists():
        print(f"{ENV_EXAMPLE} does not exist. Cannot create .env.")
        return

    # Read the example file
    content = ENV_EXAMPLE.read_text()

    # Replace placeholder with absolute path
    content = content.replace("/path/to/analytics_system/.dagster_home", str(DAGSTER_HOME.resolve()))

    # Write to .env, but don't overwrite unless confirmed
    if ENV_FILE.exists() and not no_prompt:
        confirm = input(f"{ENV_FILE} already exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborting .env creation.")
            return

    ENV_FILE.write_text(content)
    print(f"Created {ENV_FILE} with DAGSTER_HOME={DAGSTER_HOME.resolve()}")

    print("Creating duckdb warehouse placeholders...")

    for location in [
        INGEST_CALLS_WAREHOUSE,
        INGEST_CRM_WAREHOUSE,
        INGEST_SURVEYS_WAREHOUSE,
        DEV_WAREHOUSE_PATH,
        PROD_WAREHOUSE_PATH,
    ]:
        con = duckdb.connect(database=str(location))
        con.close()
        print(f"Created {location}")

    if not PROFILES_YAML_EXAMPLE.exists():
        print(f"{PROFILES_YAML_EXAMPLE} does not exist. Cannot create profiles.yml.")
        return

    content = PROFILES_YAML_EXAMPLE.read_text()
    content = content.replace(
        "/path/to/analytics-engineering-modern-stack/data/warehouse/warehouse_dev.duckdb",
        str(DEV_WAREHOUSE_PATH.resolve()),
    ).replace(
        "/path/to/analytics-engineering-modern-stack/data/warehouse/warehouse_prod.duckdb",
        str(PROD_WAREHOUSE_PATH.resolve()),
    ).replace(
        "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_calls.duckdb",
        str(INGEST_CALLS_WAREHOUSE.resolve()),
    ).replace(
        "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_crm.duckdb",
        str(INGEST_CRM_WAREHOUSE.resolve())
    ).replace(
        "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_surveys.duckdb",
        str(INGEST_SURVEYS_WAREHOUSE.resolve())
    )

    if PROFILES_YAML_FILE.exists() and not no_prompt:
        confirm = input(f"{PROFILES_YAML_FILE} already exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborting profiles.yml creation.")
            return

    PROFILES_YAML_FILE.write_text(content)
    print(f"Created {PROFILES_YAML_FILE} with local warehouses.")


def _confirm_and_delete(path: Path, preserve=None):
    if preserve is None:
        preserve = []
    if not path.exists():
        print(f"SKIPPING: {path} does not exist")
        return
    for item in path.iterdir():
        if item.name in preserve:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()

    for item in preserve:
        print(f"PRESERVED: {path / item}")

    print(f"COMPLETED: Cleaned {path}")

def _resolve_path(p: str | Path) -> Path:
    """Resolve relative paths to be repo-root relative (next to manage.py)."""
    p = Path(p)
    return p if p.is_absolute() else BASE_DIR / p


def reset_dagster():
    """Reset Dagster state (keep dagster.yaml)."""
    _confirm_and_delete(DAGSTER_HOME, preserve=["dagster.yaml"])


def reset_dlt():
    """Reset dlt state (clear pipelines)."""
    _confirm_and_delete(DLT_DIR)


def reset_warehouse():
    """Reset DuckDB warehouse state."""
    _confirm_and_delete(WAREHOUSE_DIR)

def reset_source_data():
    """Reset source data."""
    _confirm_and_delete(DATA_DIR, preserve=["warehouse"])


def reset_metabase_data():
    """Reset metabase data."""
    _confirm_and_delete(METABASE_DATA_PATH)

def generate_source_data(args):
    """
    Run the call center simulation.
    Optionally pass overrides as a dict {param: value}.
    """
    overrides = {
        "seed_output_dir": _resolve_path("./call_center/seeds"),
        "parquet_output_dir": _resolve_path("./data"),
    }
    if args.global_start_date:
        overrides["global_start_date"] = datetime.datetime.strptime(args.global_start_date, "%Y-%m-%d").date()

    if args.global_end_date:
        overrides["global_end_date"] = datetime.datetime.strptime(args.global_end_date, "%Y-%m-%d").date()

    run_simulation(**overrides)


def main():
    parser = argparse.ArgumentParser(
        description="Manage and reset project state (Dagster, dlt, warehouse)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    simulate_parser = subparsers.add_parser(
        "generate-source-data", help="Generates source data simulating the call center"
    )
    simulate_parser.add_argument("--global-start-date", type=str, help="Global start date")
    simulate_parser.add_argument("--global-end-date", type=str, help="Global end date")

    init_env_parser = subparsers.add_parser(
        "init-env", help="Initialize environment (.env files and initial database destinations)"
    )
    init_env_parser.add_argument("--no-prompt", action="store_true", help="Do not prompt before overwriting files")

    reset_parser = subparsers.add_parser("reset", help="Reset state for the various targets")
    reset_parser.add_argument(
        "targets",
        nargs="+",
        choices=["dagster", "dlt", "warehouse", "source-data", "metabase",  "all"],
        help="Which components to reset",
    )

    subparsers.add_parser("week1", help="Reset state for week1 assignments")

    args = parser.parse_args()

    if args.command == "reset":
        if "all" in args.targets:
            reset_dagster()
            reset_dlt()
            reset_warehouse()
            reset_source_data()
            reset_metabase_data()
        else:
            for target in args.targets:
                {
                    "dagster": reset_dagster,
                    "dlt": reset_dlt,
                    "warehouse": reset_warehouse,
                    "source-data": reset_source_data,
                    "metabase": reset_metabase_data
                }[target]()
    elif args.command == "generate-source-data":
        generate_source_data(args)
    elif args.command == "init-env":
        init_env(no_prompt=getattr(args, "no_prompt", False))
    elif args.command == "week1":
        reset_dagster()
        reset_dlt()
        reset_warehouse()
        reset_source_data()
        reset_metabase_data()
        init_env(no_prompt=True)
        # generate_source_data(argparse.Namespace(global_start_date="2025-02-01", global_end_date="2025-03-20"))
        # kickoff jobs?
        # import subprocess
        # def run_analytics_system_command():
        #     cmd = [
        #         "uv",
        #         "run",
        #         "dg",
        #         "launch",
        #         "--assets",
        #         "dlt_filesystem_calls_source_calls",
        #         "--partition-range",
        #         "2025-01-06...2025-01-12",
        #     ]
        #     subprocess.run(cmd, cwd=BASE_DIR / "analytics_system")
        # run_analytics_system_command()


if __name__ == "__main__":
    main()
