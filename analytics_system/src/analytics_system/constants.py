import os.path
import pathlib

# Had to build absolute paths to route sources and destinations regardless of where this code is run
# (ex: in dagster vs. locally)

repo_dir = pathlib.Path(__file__).parent.parent.parent.resolve()
warehouse_dir = repo_dir / ".data" / "warehouse"
warehouse_dir.mkdir(parents=True, exist_ok=True)

WAREHOUSE_FILE_ABS_PATH = os.path.abspath(warehouse_dir / "data_warehouse.duckdb")

SOURCE_DATA_DIR_PATH = pathlib.Path(__file__).parent.parent.parent.parent.resolve() / "data"
