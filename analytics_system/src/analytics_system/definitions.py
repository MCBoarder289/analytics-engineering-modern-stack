import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from .constants import dbt_project_dir
from .defs.dbt_assets.assets import dbt_analytics, dbt_seeds
from .defs.filesystem_duckdb_ingest.loads import calls_ingestion, crm_ingestion, surveys_ingestion


@dg.definitions
def defs():
    return dg.Definitions(
        assets=[
            # dlt assets
            calls_ingestion,
            crm_ingestion,
            surveys_ingestion,
            # dbt assets
            dbt_analytics,
            dbt_seeds,
        ],
        resources={
            "dlt": DagsterDltResource(),
            "dbt": DbtCliResource(project_dir=dbt_project_dir),
        },
    )
