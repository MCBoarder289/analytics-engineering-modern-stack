from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from dagster_dlt import DagsterDltResource

from .defs.filesystem_duckdb_ingest.loads import calls_ingestion, crm_ingestion, surveys_ingestion

dlt_resource = DagsterDltResource()

@definitions
def defs():
    default_defs = load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)

    dlt_defs = Definitions(
        assets=[
            calls_ingestion,
            crm_ingestion,
            surveys_ingestion,
        ],
        resources={
            "dlt": dlt_resource
        }
    )

    return Definitions.merge(default_defs, dlt_defs)
