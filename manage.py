
import argparse
import shutil
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.resolve()
DAGSTER_HOME = BASE_DIR / "analytics_system" / ".dagster_home"
DLT_DIR = BASE_DIR / ".dlt"
WAREHOUSE_DIR = BASE_DIR / "data" / "warehouse"
ENV_EXAMPLE = BASE_DIR / "analytics_system" / ".env.example"
ENV_FILE = BASE_DIR / "analytics_system" / ".env"

INGEST_CALLS_WAREHOUSE = WAREHOUSE_DIR / "ingest_calls.duckdb"
INGEST_CRM_WAREHOUSE = WAREHOUSE_DIR / "ingest_crm.duckdb"
INGEST_SURVEYS_WAREHOUSE = WAREHOUSE_DIR / "ingest_surveys.duckdb"

def init_env():
    if not ENV_EXAMPLE.exists():
        print(f"{ENV_EXAMPLE} does not exist. Cannot create .env.")
        return

    # Read the example file
    content = ENV_EXAMPLE.read_text()

    # Replace placeholder with absolute path
    content = content.replace("/path/to/analytics_system/.dagster_home", str(DAGSTER_HOME.resolve()))

    # Write to .env, but don't overwrite unless confirmed
    if ENV_FILE.exists():
        confirm = input(f"{ENV_FILE} already exists. Overwrite? [y/N]: ").strip().lower()
        if confirm.lower() != "y":
            print("Aborting .env creation.")
            return

    ENV_FILE.write_text(content)
    print(f"Created {ENV_FILE} with DAGSTER_HOME={DAGSTER_HOME.resolve()}")

    print("Creating duckdb warehouse placeholders...")

    for location in [INGEST_CALLS_WAREHOUSE, INGEST_CRM_WAREHOUSE, INGEST_SURVEYS_WAREHOUSE]:
        con = duckdb.connect(database=str(location))
        con.close()
        print(f"Created {location}")


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
    print(f"COMPLETED: Cleaned {path}")


def reset_dagster():
    """Reset Dagster state (keep dagster.yaml)."""
    _confirm_and_delete(DAGSTER_HOME, preserve=["dagster.yaml"])


def reset_dlt():
    """Reset dlt state (clear pipelines)."""
    _confirm_and_delete(DLT_DIR)


def reset_warehouse():
    """Reset DuckDB warehouse state."""
    _confirm_and_delete(WAREHOUSE_DIR)


def reset_all():
    """Reset everything: dagster, dlt, and warehouse."""
    reset_dagster()
    reset_dlt()
    reset_warehouse()


def main():
    parser = argparse.ArgumentParser(
        description="Manage and reset project state (Dagster, dlt, warehouse)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("reset-dagster", help="Reset Dagster state")
    subparsers.add_parser("reset-dlt", help="Reset dlt state")
    subparsers.add_parser("reset-warehouse", help="Reset warehouse state")
    subparsers.add_parser("reset-all", help="Reset everything")

    subparsers.add_parser("init-env", help="Create .env from .env.example with absolute paths")


    args = parser.parse_args()

    if args.command == "reset-dagster":
        reset_dagster()
    elif args.command == "reset-dlt":
        reset_dlt()
    elif args.command == "reset-warehouse":
        reset_warehouse()
    elif args.command == "reset-all":
        reset_all()
    elif args.command == "init-env":
        init_env()


if __name__ == "__main__":
    main()
