from dagster import Definitions, definitions
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from .constants import dbt_project_dir
from .defs.dbt_assets.assets import dbt_analytics, dbt_seeds
from .defs.filesystem_duckdb_ingest.loads import calls_ingestion, crm_ingestion, surveys_ingestion

dlt_resource = DagsterDltResource()
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)


@definitions
def defs():
    dlt_defs = Definitions(
        assets=[
            calls_ingestion,
            crm_ingestion,
            surveys_ingestion,
        ],
        resources={
            "dlt": dlt_resource,
        },
    )

    dbt_defs = Definitions(
        assets=[
            dbt_analytics,
            dbt_seeds,
        ],
        resources={
            "dbt": dbt_resource,
        },
    )

    return Definitions.merge(dbt_defs, dlt_defs)
