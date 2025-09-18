import os
from typing import Optional

import dlt
from dagster import AssetExecutionContext, BackfillPolicy, DailyPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt.sources.filesystem import filesystem, read_parquet

from analytics_system.constants import source_data_dir, warehouse_dir

daily_partitions = DailyPartitionsDefinition("2025-01-01", "2025-03-31")



@dlt.source
def filesystem_calls_source(date_partition: Optional[str] = None):
    @dlt.resource
    def raw_calls():
        if date_partition:
            abs_path = os.path.abspath(os.path.join(source_data_dir, f"calls/day={date_partition}/"))
            partition_path = f"file://{abs_path}"

            # Need to add the type ignore to the method chaining that dlt supports (piping into read_parquet())
            yield from (filesystem(partition_path, file_glob="*.parquet") | read_parquet())  # type: ignore

    return raw_calls


@dlt_assets(
    dlt_source=filesystem_calls_source(),
    name="calls_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="raw_calls_ingestion",  # TODO: Figure out
        dataset_name="raw_calls",
        destination=dlt.destinations.duckdb(os.path.abspath(os.path.join(warehouse_dir, "raw_calls_ingestion.duckdb")))
    ),
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    group_name="raw_ingestion",
)
def calls_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    date = context.partition_key
    yield from dlt.run(context=context, dlt_source=filesystem_calls_source(date_partition=date))


# Testing the dlt code runs locally
# Lesson learned: Running from dagster has a whole different starting location, so we needed to perform a more robust
# absolute pathing structure. In a production deployment, this would likely be a remote location vs. a local one.

# pipeline = dlt.pipeline(
#         pipeline_name="raw_calls_ingestion",
#         dataset_name="raw_calls",
#         destination=dlt.destinations.duckdb(
#             os.path.abspath(os.path.join(warehouse_dir, "raw_calls_ingestion.duckdb"))
#         )
#     )

# load_info = pipeline.run(filesystem_calls_source(date_partition="2025-01-01"))
# # with pipeline.sql_client() as client:
# #     with client.execute_query("SELECT * FROM raw_calls;") as cursor:
# #         print(cursor.fetchall())
# print(load_info)