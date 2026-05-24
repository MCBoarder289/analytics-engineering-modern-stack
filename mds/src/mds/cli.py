import argparse
import datetime
import logging
import shutil
from pathlib import Path

import duckdb

from mds.data_generation.call_center_simulation import main as run_simulation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# cli.py lives at mds/src/mds/cli.py — go up four levels to reach the repo root
BASE_DIR = Path(__file__).parents[3].resolve()
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

SCRIPTS_DIR = BASE_DIR / "scripts"
WAREHOUSE_STARTUP_TEMPLATE_FILE = SCRIPTS_DIR / "warehouse-startup-template.sql"
WAREHOUSE_STARTUP_SCRIPT = SCRIPTS_DIR / "warehouse-startup.sql"

ASSIGNMENTS_DIR = BASE_DIR / "assignments"

# Maps each module number to its live file paths (relative to BASE_DIR).
# These are the files that get replaced by stubs and restored from answers.
ASSIGNMENT_FILES: dict[int, list[str]] = {
    5: [
        "call_center/models/staging/stg_calls.sql",
        "call_center/models/staging/stg_crm.sql",
        "call_center/models/staging/stg_surveys.sql",
    ],
    6: [
        "call_center/models/data_marts/mart_surveys.sql",
        "call_center/models/data_marts/mart_first_call_resolution.sql",
    ],
    8: [
        "call_center/models/ops_analysis/daily_agent_metrics.sql",
        "call_center/models/ops_analysis/monthly_agent_metrics.sql",
        "call_center/models/ops_analysis/daily_agent_manager_metrics.sql",
        "call_center/models/ops_analysis/monthly_agent_manager_metrics.sql",
        "call_center/models/ops_analysis/monthly_manager_metrics.sql",
    ],
    9: [
        "call_center/models/staging/properties.yml",
        "call_center/models/data_marts/properties.yml",
        "call_center/models/ops_analysis/properties.yml",
    ],
}


def init_warehouse_files():
    """Create empty DuckDB files for all warehouse databases."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    for location in [
        INGEST_CALLS_WAREHOUSE,
        INGEST_CRM_WAREHOUSE,
        INGEST_SURVEYS_WAREHOUSE,
        DEV_WAREHOUSE_PATH,
        PROD_WAREHOUSE_PATH,
    ]:
        con = duckdb.connect(database=str(location))
        con.close()
        logger.info(f"Created {location}")


def _substitute_warehouse_paths(content: str) -> str:
    """Replace placeholder paths in a template with resolved absolute warehouse paths."""
    return (
        content
        .replace(
            "/path/to/analytics-engineering-modern-stack/data/warehouse/warehouse_dev.duckdb",
            str(DEV_WAREHOUSE_PATH.resolve()),
        )
        .replace(
            "/path/to/analytics-engineering-modern-stack/data/warehouse/warehouse_prod.duckdb",
            str(PROD_WAREHOUSE_PATH.resolve()),
        )
        .replace(
            "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_calls.duckdb",
            str(INGEST_CALLS_WAREHOUSE.resolve()),
        )
        .replace(
            "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_crm.duckdb",
            str(INGEST_CRM_WAREHOUSE.resolve()),
        )
        .replace(
            "/path/to/analytics-engineering-modern-stack/data/warehouse/ingest_surveys.duckdb",
            str(INGEST_SURVEYS_WAREHOUSE.resolve()),
        )
    )


def init_env(no_prompt: bool = False) -> None:
    if not ENV_EXAMPLE.exists():
        logger.info(f"{ENV_EXAMPLE} does not exist. Cannot create .env.")
        return

    # Read the example file
    content = ENV_EXAMPLE.read_text(encoding="utf-8")

    # Replace placeholder with absolute path
    content = content.replace("/path/to/analytics_system/.dagster_home", str(DAGSTER_HOME.resolve()))

    # Write to .env, but don't overwrite unless confirmed
    if ENV_FILE.exists() and not no_prompt:
        confirm = input(f"{ENV_FILE} already exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != "y":
            logger.info("Aborting .env creation.")
            return

    ENV_FILE.write_text(content, encoding="utf-8")
    logger.info(f"Created {ENV_FILE} with DAGSTER_HOME={DAGSTER_HOME.resolve()}")

    logger.info("Creating duckdb warehouse placeholders...")
    init_warehouse_files()

    logger.info("Generating duckdb warehouse startup script...")

    content = WAREHOUSE_STARTUP_TEMPLATE_FILE.read_text(encoding="utf-8")
    content = _substitute_warehouse_paths(content)

    WAREHOUSE_STARTUP_SCRIPT.write_text(content, encoding="utf-8")

    logger.info(f"Created {WAREHOUSE_STARTUP_SCRIPT} with local warehouses.")

    if not PROFILES_YAML_EXAMPLE.exists():
        logger.error(f"{PROFILES_YAML_EXAMPLE} does not exist. Cannot create profiles.yml.")
        return

    content = PROFILES_YAML_EXAMPLE.read_text(encoding="utf-8")
    content = _substitute_warehouse_paths(content)

    if PROFILES_YAML_FILE.exists() and not no_prompt:
        confirm = input(f"{PROFILES_YAML_FILE} already exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != "y":
            logger.info("Aborting profiles.yml creation.")
            return

    PROFILES_YAML_FILE.write_text(content, encoding="utf-8")
    logger.info(f"Created {PROFILES_YAML_FILE} with local warehouses.")


def _delete_contents(path: Path, preserve: list[str] | None = None) -> None:
    if preserve is None:
        preserve = []
    if not path.exists():
        logger.info(f"SKIPPING: {path} does not exist")
        return
    for item in path.iterdir():
        if item.name in preserve:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()

    for item in preserve:
        logger.info(f"PRESERVED: {path / item}")

    logger.info(f"COMPLETED: Cleaned {path}")


def _resolve_path(p: str | Path) -> Path:
    """Resolve relative paths to be repo-root relative."""
    p = Path(p)
    return p if p.is_absolute() else BASE_DIR / p


def reset_dagster():
    """Reset Dagster state (keep dagster.yaml)."""
    _delete_contents(DAGSTER_HOME, preserve=["dagster.yaml"])


def reset_dlt():
    """Reset dlt state (clear pipelines)."""
    _delete_contents(DLT_DIR)


def reset_warehouse():
    """Reset DuckDB warehouse state and re-initialize empty database files."""
    _delete_contents(WAREHOUSE_DIR)
    logger.info("Re-initializing empty warehouse database files...")
    init_warehouse_files()


def reset_source_data():
    """Reset source data."""
    _delete_contents(DATA_DIR, preserve=["warehouse"])


def reset_metabase_data():
    """Reset metabase data."""
    _delete_contents(METABASE_DATA_PATH)


def source_data_status() -> str:
    """
    Check whether source data parquet files exist for all three datasets.

    Returns:
        'complete'  — all three datasets have parquet files
        'partial'   — at least one dataset has files but another is missing/empty
                      (running the simulation again would append and create duplicates)
        'missing'   — none of the datasets have any parquet files
    """
    datasets = ("calls", "crm", "surveys")
    has_data = []
    for dataset in datasets:
        dataset_dir = DATA_DIR / dataset
        has_data.append(dataset_dir.exists() and any(dataset_dir.rglob("*.parquet")))

    if all(has_data):
        return "complete"
    if any(has_data):
        return "partial"
    return "missing"


def generate_source_data(args: argparse.Namespace) -> None:
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


def inject_duplicate_parquets(num_days: int = 3) -> None:
    """Copies source parquet files under new names to simulate a dlt re-ingestion event.

    dlt tracks ingested files via a ``file_url`` cursor. Copying a file under a new name
    causes dlt to treat it as a previously unseen file and re-ingest its rows, creating
    duplicates in the raw DuckDB tables. Students observe the resulting unique-test failures
    in the mart layer and fix them by implementing the deduplication CTE in staging.
    """
    for dataset in ("calls", "crm", "surveys"):
        dataset_dir = DATA_DIR / dataset
        if not dataset_dir.exists():
            logger.warning(f"Dataset directory {dataset_dir} does not exist — skipping.")
            continue

        day_dirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir() and d.name.startswith("day="))
        for day_dir in day_dirs[:num_days]:
            originals = [f for f in day_dir.glob("*.parquet") if "_dup" not in f.stem]
            for src in originals:
                dst = day_dir / (src.stem + "_dup" + src.suffix)
                if not dst.exists():
                    shutil.copy2(src, dst)
                    logger.info(f"Created duplicate: {dst.relative_to(BASE_DIR)}")
                else:
                    logger.info(f"Duplicate already exists (skipping): {dst.relative_to(BASE_DIR)}")


def cleanup_duplicate_parquets() -> None:
    """Removes all duplicate parquet files created by inject_duplicate_parquets.

    Useful for resetting the source data directory back to a clean state after
    completing the Module 5 deduplication exercise.
    """
    removed = 0
    for dataset in ("calls", "crm", "surveys"):
        dataset_dir = DATA_DIR / dataset
        if not dataset_dir.exists():
            continue
        for dup_file in dataset_dir.rglob("*_dup.parquet"):
            dup_file.unlink()
            logger.info(f"Removed duplicate: {dup_file.relative_to(BASE_DIR)}")
            removed += 1
    if removed == 0:
        logger.info("No duplicate parquet files found.")
    else:
        logger.info(f"Removed {removed} duplicate parquet file(s).")


def sync_answers() -> None:
    """Copies the current live model files into each module's answers/ directory.

    Run this after editing any live model file to keep the assignment answer keys in sync:
        uv run mds sync-answers
    """
    for module_num, file_paths in ASSIGNMENT_FILES.items():
        for rel_path in file_paths:
            src = BASE_DIR / rel_path
            dst = ASSIGNMENTS_DIR / f"module{module_num}" / "answers" / rel_path
            if not src.exists():
                logger.warning(f"Source file not found, skipping: {src}")
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            logger.info(f"Synced answer: {src.relative_to(BASE_DIR)} -> {dst.relative_to(BASE_DIR)}")


def setup_assignment(module: int, no_reset: bool = False) -> None:
    """Installs stub files for the given module and optionally resets pipeline state."""
    if module not in ASSIGNMENT_FILES:
        logger.error(f"Module {module} is not configured. Available modules: {sorted(ASSIGNMENT_FILES)}")
        return

    stubs_dir = ASSIGNMENTS_DIR / f"module{module}" / "stubs"

    data_status = source_data_status()
    if data_status == "missing":
        logger.warning("No source data found. Running generate-source-data with default settings...")
        run_simulation(
            seed_output_dir=_resolve_path("./call_center/seeds"),
            parquet_output_dir=_resolve_path("./data"),
        )
    elif data_status == "partial":
        logger.error(
            "Source data is incomplete — some datasets exist but others are missing.\n"
            "Running the simulation again would append duplicate records.\n"
            "To fix: run 'uv run mds reset source-data' then "
            "'uv run mds generate-source-data', then retry."
        )
        return

    logger.info(f"Installing stubs for Module {module}...")
    for rel_path in ASSIGNMENT_FILES[module]:
        src = stubs_dir / rel_path
        dst = BASE_DIR / rel_path
        if not src.exists():
            logger.error(f"Stub file not found: {src}")
            continue
        shutil.copy2(src, dst)
        logger.info(f"Installed stub: {dst.relative_to(BASE_DIR)}")

    if module == 5:
        logger.info("Injecting duplicate parquet files for the deduplication exercise...")
        inject_duplicate_parquets()

    if not no_reset:
        confirm = input("\nReset Dagster, dlt, and warehouse state? [Y/n]: ").strip().lower()
        if confirm in ("", "y"):
            reset_dagster()
            reset_dlt()
            reset_warehouse()
            init_env(no_prompt=True)

    readme = ASSIGNMENTS_DIR / f"module{module}" / "README.md"
    logger.info(f"\nAssignment ready! Read the instructions at:\n  {readme}")


def restore_assignment(module: int, no_reset: bool = False) -> None:
    """Restores the answer key files for the given module over the live files.

    If students want to see the finished solution (or recover from a broken state), this
    copies the answer files back. Run sync-answers first if the live models have changed.
    """
    if module not in ASSIGNMENT_FILES:
        logger.error(f"Module {module} is not configured. Available modules: {sorted(ASSIGNMENT_FILES)}")
        return

    answers_dir = ASSIGNMENTS_DIR / f"module{module}" / "answers"
    logger.info(f"Restoring answer key for Module {module}...")
    for rel_path in ASSIGNMENT_FILES[module]:
        src = answers_dir / rel_path
        dst = BASE_DIR / rel_path
        if not src.exists():
            logger.error(f"Answer file not found: {src}\n  Run 'uv run mds sync-answers' to populate the answer keys.")
            continue
        shutil.copy2(src, dst)
        logger.info(f"Restored: {dst.relative_to(BASE_DIR)}")

    cleanup_duplicate_parquets()

    if not no_reset:
        confirm = input("\nReset Dagster, dlt, and warehouse state? [Y/n]: ").strip().lower()
        if confirm in ("", "y"):
            reset_dagster()
            reset_dlt()
            reset_warehouse()
            init_env(no_prompt=True)

    logger.info(f"Module {module} answer key applied.")


def restore_all_assignments(no_reset: bool = False) -> None:
    """Restores answer key files for all configured modules.

    Useful when a student has set up multiple assignments without restoring and
    needs to get back to a clean known-good state.
    """
    logger.info("Restoring answer keys for all modules...")
    for module in sorted(ASSIGNMENT_FILES):
        answers_dir = ASSIGNMENTS_DIR / f"module{module}" / "answers"
        logger.info(f"Restoring Module {module}...")
        for rel_path in ASSIGNMENT_FILES[module]:
            src = answers_dir / rel_path
            dst = BASE_DIR / rel_path
            if not src.exists():
                logger.error(
                    f"Answer file not found: {src}\n  Run 'uv run mds sync-answers' to populate the answer keys."
                )
                continue
            shutil.copy2(src, dst)
            logger.info(f"  Restored: {dst.relative_to(BASE_DIR)}")

    cleanup_duplicate_parquets()

    if not no_reset:
        confirm = input("\nReset Dagster, dlt, and warehouse state? [Y/n]: ").strip().lower()
        if confirm in ("", "y"):
            reset_dagster()
            reset_dlt()
            reset_warehouse()
            init_env(no_prompt=True)

    logger.info("All modules restored.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage and reset project state (Dagster, dlt, warehouse).")
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
        choices=["dagster", "dlt", "warehouse", "source-data", "metabase", "all"],
        help="Which components to reset",
    )

    # subparsers.add_parser("week1", help="Reset state for week1 assignments")

    assignment_parser = subparsers.add_parser(
        "assignment", help="Install assignment stubs or restore answer key for a given module"
    )
    assignment_parser.add_argument(
        "--module",
        type=int,
        choices=sorted(ASSIGNMENT_FILES),
        help="Module number to set up",
    )
    assignment_parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore the answer key files instead of installing stubs",
    )
    assignment_parser.add_argument(
        "--restore-all",
        action="store_true",
        help="Restore answer keys for ALL modules at once (useful for recovering from a broken state)",
    )
    assignment_parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Skip the prompt to reset Dagster/dlt/warehouse state",
    )

    subparsers.add_parser("sync-answers", help="Sync live model files into assignments/moduleN/answers/")
    subparsers.add_parser(
        "cleanup-dupes", help="Remove duplicate parquet files created by the Module 5 assignment setup"
    )

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
                    "metabase": reset_metabase_data,
                }[target]()
    elif args.command == "generate-source-data":
        generate_source_data(args)
    elif args.command == "init-env":
        init_env(no_prompt=args.no_prompt)
    elif args.command == "assignment":
        no_reset = args.no_reset
        if args.restore_all:
            restore_all_assignments(no_reset=no_reset)
        elif args.module is None:
            assignment_parser.error("--module is required unless --restore-all is specified")
        elif args.restore:
            restore_assignment(module=args.module, no_reset=no_reset)
        else:
            setup_assignment(module=args.module, no_reset=no_reset)
    elif args.command == "sync-answers":
        sync_answers()
    elif args.command == "cleanup-dupes":
        cleanup_duplicate_parquets()


if __name__ == "__main__":
    main()
