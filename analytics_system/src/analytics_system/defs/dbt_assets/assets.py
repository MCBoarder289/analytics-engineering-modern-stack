import json

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

from analytics_system.constants import GLOBAL_END_DATE, GLOBAL_START_DATE, dbt_project

INCREMENTAL_SELECTOR = "config.materialized:incremental"

daily_partition = dg.DailyPartitionsDefinition(start_date=GLOBAL_START_DATE, end_date=GLOBAL_END_DATE)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude=INCREMENTAL_SELECTOR,
    pool="duckdb_dbt",
)
def dbt_seeds(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts().fetch_column_metadata()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partition,
    pool="duckdb_dbt",
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # Need to have a build command in case there's no partitions called (ex: running asset checks from asset def page)
    if context.has_partition_key_range or context.has_partition_key:
        time_window = context.partition_time_window
        dbt_vars = {
            "start_date": time_window.start.strftime("%Y-%m-%d"),
            "end_date": time_window.end.strftime("%Y-%m-%d"),
        }
        cli_args = ["build", "--vars", json.dumps(dbt_vars)]
    else:
        cli_args = ["build"]

    yield from dbt.cli(args=cli_args, context=context).stream().fetch_row_counts().fetch_column_metadata()
