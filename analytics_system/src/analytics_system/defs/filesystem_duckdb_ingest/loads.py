import os
from typing import Optional

import dlt
from dagster import AssetExecutionContext, BackfillPolicy, DailyPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt.sources.filesystem import filesystem, read_parquet

from analytics_system.constants import (
    DLT_STATE_LOCATION_ABS_PATH,
    INGEST_CALLS_ABS_PATH,
    INGEST_CRM_ABS_PATH,
    INGEST_SURVEYS_ABS_PATH,
    SOURCE_DATA_DIR_PATH,
)

daily_partitions = DailyPartitionsDefinition("2025-01-01", "2025-03-31")

def parquet_day_partition(dataset: str, date_partition: str):
    """Helper function to dynamically produce the boilerplate glob access of the raw source files"""
    abs_path = os.path.abspath(os.path.join(SOURCE_DATA_DIR_PATH, f"{dataset}/day={date_partition}/"))
    partition_path = f"file://{abs_path}"
    # Need to add the "type: ignore" to the method chaining that dlt supports (piping into read_parquet())
    return filesystem(partition_path, file_glob="*.parquet") | read_parquet()  # type: ignore


@dlt.source
def filesystem_calls_source(date_partition: Optional[str] = None):
    @dlt.resource
    def calls():
        if date_partition:
            yield from parquet_day_partition(dataset="calls", date_partition=date_partition)

    return calls


@dlt.source
def filesystem_crm_source(date_partition: Optional[str] = None):
    @dlt.resource
    def crm():
        if date_partition:
            yield from parquet_day_partition(dataset="crm", date_partition=date_partition)

    return crm

@dlt.source
def filesystem_surveys_source(date_partition: Optional[str] = None):
    @dlt.resource
    def surveys():
        if date_partition:
            yield from parquet_day_partition(dataset="surveys", date_partition=date_partition)

    return surveys


@dlt_assets(
    dlt_source=filesystem_calls_source(),
    name="calls_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="raw_calls_ingestion",
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_calls",
        destination=dlt.destinations.duckdb(INGEST_CALLS_ABS_PATH),
    ),
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    group_name="raw_ingestion",
    pool="duckdb_ingest_calls",
)
def calls_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    date = context.partition_key
    yield from dlt.run(context=context, dlt_source=filesystem_calls_source(date_partition=date))


@dlt_assets(
    dlt_source=filesystem_crm_source(),
    name="crm_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="raw_crm_ingestion",
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_crm",
        destination=dlt.destinations.duckdb(INGEST_CRM_ABS_PATH)
    ),
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    group_name="raw_ingestion",
    pool="duckdb_ingest_crm",
)
def crm_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    date = context.partition_key
    yield from dlt.run(context=context, dlt_source=filesystem_crm_source(date_partition=date))


@dlt_assets(
    dlt_source=filesystem_surveys_source(),
    name="survey_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="raw_surveys_ingestion",
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_surveys",
        destination=dlt.destinations.duckdb(INGEST_SURVEYS_ABS_PATH),
    ),
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    group_name="raw_ingestion",
    pool="duckdb_ingest_surveys",
)
def surveys_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    date = context.partition_key
    yield from dlt.run(context=context, dlt_source=filesystem_surveys_source(date_partition=date))


# TODO: Configure concurrency so that it doesn't fail.
# Concurrency notes: I was able to materialize all 89 partitions of a single asset pretty easily without any
# concurrency limits. However, when I tried to backfill all 3 for all the same partitions, I ran into
# a bunch of failures. Looking at the dagster run logs, if I filtered for the first failure, it was due to this error:
# _duckdb.IOException: IO Error: Could not set lock on file ".../data_warehouse.duckdb": Conflicting lock is held in
# .../Python (PID 22584) by user michaelchapman. See also https://duckdb.org/docs/stable/connect/concurrency
# In the end, it was about ~30 something partitions that were completed per data source, so likely that's some limit
# given my machine.
#
# I was able to re-run the failed paritions, and all but one failed, but that certainly caused dupes in the source data
# It was nice to know that dagster pretty gracefully only ran jobs for failed partitions within each source, which in
# theory should not have produced dupes from the source, but I bet some of the partition ranges overlapped or something?

# Putting them on pools and making the pool limit value 1 made this complete successfully without the write errors.
# Removing concurrency increased the pipeline time to bout 8.5 minutes for the entire dataset (not great).
# This requires going into the UI because you can't set that in the dagster.yaml, except for a default for all




# TODO: Figure out if any of this should be unit tested

# Testing the dlt code runs locally
# Lesson learned: Running from dagster has a whole different starting location, so we needed to perform a more robust
# absolute pathing structure. In a production deployment, this would likely be a remote location vs. a local one.

# pipeline = dlt.pipeline(
#     pipeline_name="raw_calls_ingestion",
#     dataset_name="raw_calls",
#     destination=dlt.destinations.duckdb(WAREHOUSE_FILE_ABS_PATH),
# )
#
# load_info = pipeline.run(filesystem_calls_source(date_partition="2025-01-01"))
# print(load_info)
#
# pipeline = dlt.pipeline(
#     pipeline_name="raw_crm_ingestion",
#     dataset_name="raw_crm",
#     destination=dlt.destinations.duckdb(WAREHOUSE_FILE_ABS_PATH),
# )
#
# load_info = pipeline.run(filesystem_crm_source(date_partition="2025-01-01"))
# print(load_info)
#
# pipeline = dlt.pipeline(
#     pipeline_name="raw_surveys_ingestion",
#     dataset_name="raw_surveys",
#     destination=dlt.destinations.duckdb(WAREHOUSE_FILE_ABS_PATH),
# )
#
# load_info = pipeline.run(filesystem_surveys_source(date_partition="2025-01-01"))
#
# # with pipeline.sql_client() as client:
# #     with client.execute_query("SELECT * FROM raw_calls;") as cursor:
# #         print(cursor.fetchall())
# print(load_info)