import json
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from analytics_system.constants import DAILY_PARTITION, dbt_project

INCREMENTAL_SELECTOR = "config.materialized:incremental"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: DbtProject | None,
    ) -> dg.AssetSpec:
        # Let the base translator build the default spec and its native dbt dependencies
        spec = super().get_asset_spec(manifest, unique_id, project)

        if unique_id.startswith("seed."):
            dlt_upstream_keys = [
                dg.AssetKey("dlt_filesystem_calls_source_calls"),
                dg.AssetKey("dlt_filesystem_calls_source_crm"),
                dg.AssetKey("dlt_filesystem_calls_source_surveys"),
            ]

            new_deps = [dg.AssetDep(asset=key) for key in dlt_upstream_keys]

            combined_deps = list(spec.deps) + new_deps

            return spec.replace_attributes(deps=combined_deps)

        return spec


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude=INCREMENTAL_SELECTOR,
    pool="duckdb_dbt",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_seeds(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts().fetch_column_metadata()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    partitions_def=DAILY_PARTITION,
    pool="duckdb_dbt",
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    cli_args = ["build"]
    # Need to have a build command in case there's no partitions called (ex: running asset checks from asset def page)
    if context.has_partition_key_range or context.has_partition_key:
        time_window = context.partition_time_window
        dbt_vars = {
            "start_date": time_window.start.strftime("%Y-%m-%d"),
            "end_date": time_window.end.strftime("%Y-%m-%d"),
        }
        cli_args.extend(["--vars", json.dumps(dbt_vars)])

    yield from dbt.cli(args=cli_args, context=context).stream().fetch_row_counts().fetch_column_metadata()
