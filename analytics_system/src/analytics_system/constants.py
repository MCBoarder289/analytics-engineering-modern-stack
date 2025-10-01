import os.path
import pathlib

from dagster_dbt import DbtProject

# Had to build absolute paths to route sources and destinations regardless of where this code is run
# (ex: in dagster vs. locally)

repo_dir = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
warehouse_dir = repo_dir / "data" / "warehouse"
warehouse_dir.mkdir(parents=True, exist_ok=True)

dlt_dir = repo_dir / ".dlt"
dlt_dir.mkdir(parents=True, exist_ok=True)

DLT_STATE_LOCATION_ABS_PATH = os.path.abspath(dlt_dir)

dbt_project_dir = pathlib.Path(__file__).absolute().parent.parent.parent.parent / "call_center"
dbt_project = DbtProject(project_dir=dbt_project_dir)
dbt_project.prepare_if_dev()  # Had to put it here to make sure it wasn't refreshing/rebuilding dbt too often

INGEST_CALLS_ABS_PATH = os.path.abspath(warehouse_dir / "ingest_calls.duckdb")
INGEST_CRM_ABS_PATH = os.path.abspath(warehouse_dir / "ingest_crm.duckdb")
INGEST_SURVEYS_ABS_PATH = os.path.abspath(warehouse_dir / "ingest_surveys.duckdb")

WAREHOUSE_FILE_ABS_PATH = os.path.abspath(warehouse_dir / "warehouse_dev.duckdb")

SOURCE_DATA_DIR_PATH = pathlib.Path(__file__).parent.parent.parent.parent.resolve() / "data"

GLOBAL_START_DATE = "2025-01-01"
GLOBAL_END_DATE = "2025-04-01"  # plus one from the data's largest date to line partitions up
