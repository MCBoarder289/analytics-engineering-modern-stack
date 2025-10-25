import os
from datetime import datetime, timedelta

import dlt
from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt.sources.filesystem import filesystem, read_parquet

from analytics_system.constants import (
    DLT_STATE_LOCATION_ABS_PATH,
    GLOBAL_END_DATE,
    GLOBAL_START_DATE,
    INGEST_CALLS_ABS_PATH,
    INGEST_CRM_ABS_PATH,
    INGEST_SURVEYS_ABS_PATH,
    SOURCE_DATA_DIR_PATH,
)

daily_partitions = DailyPartitionsDefinition(start_date=GLOBAL_START_DATE, end_date=GLOBAL_END_DATE)

dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True  # TODO: Figure out why this is needed vs. config.toml

def parquet_day_partition(dataset: str, date_partition: str):
    """Helper function to dynamically produce the boilerplate glob access of the raw source files"""
    abs_path = os.path.abspath(os.path.join(SOURCE_DATA_DIR_PATH, f"{dataset}/day={date_partition}/"))
    partition_path = f"file://{abs_path}"
    # To make the pipeline idempotent, we can add incremental by file_url,
    # but we **need** to sort by the modification date in case there are multiple files in the directory
    # Also, this assumes that we run partitions backfilled sequentially in chronological order, otherwise
    # retroactive backfills will not complete (since the alphabetical file_url cursor state won't permit an earlier one)
    fs = filesystem(
        partition_path, file_glob="*.parquet", incremental=dlt.sources.incremental("file_url", row_order="asc")
    )
    # Need to add the "type: ignore" to the method chaining that dlt supports (piping into read_parquet())
    return fs | read_parquet(use_pyarrow=True)  # type: ignore


def date_range_list(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = (end - start).days
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta + 1)]

@dlt.source
def filesystem_calls_source(date_partition: str | list[str] | None = None):
    @dlt.resource
    def calls():
        if isinstance(date_partition, str):
            yield from parquet_day_partition(dataset="calls", date_partition=date_partition)
        elif isinstance(date_partition, list):
            for date in date_partition:
                yield from parquet_day_partition(dataset="calls", date_partition=date)

    return calls


@dlt.source
def filesystem_crm_source(date_partition: str | list[str] | None = None):
    @dlt.resource
    def crm():
        if isinstance(date_partition, str):
            yield from parquet_day_partition(dataset="crm", date_partition=date_partition)
        elif isinstance(date_partition, list):
            for date in date_partition:
                yield from parquet_day_partition(dataset="crm", date_partition=date)

    return crm

@dlt.source
def filesystem_surveys_source(date_partition: str | list[str] | None = None):
    @dlt.resource
    def surveys():
        if isinstance(date_partition, str):
            yield from parquet_day_partition(dataset="surveys", date_partition=date_partition)
        elif isinstance(date_partition, list):
            for date in date_partition:
                yield from parquet_day_partition(dataset="surveys", date_partition=date)

    return surveys


@dlt_assets(
    dlt_source=filesystem_calls_source(),
    name="calls_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_calls_source",  # Needs to match the dlt.source name to avoid state issues
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_calls",
        destination=dlt.destinations.duckdb(INGEST_CALLS_ABS_PATH),
    ),
    partitions_def=daily_partitions,
    group_name="raw_ingestion",
)
def calls_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    if context.partition_key_range.start == context.partition_key_range.end:
        yield from dlt.run(context=context, dlt_source=filesystem_calls_source(date_partition=context.partition_key))
    else:
        date_list = date_range_list(context.partition_key_range.start, context.partition_key_range.end)
        yield from dlt.run(context=context, dlt_source=filesystem_calls_source(date_partition=date_list))


@dlt_assets(
    dlt_source=filesystem_crm_source(),
    name="crm_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_crm_source",  # Needs to match the dlt.source name to avoid state issues
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_crm",
        destination=dlt.destinations.duckdb(INGEST_CRM_ABS_PATH),
    ),
    partitions_def=daily_partitions,
    group_name="raw_ingestion",
)
def crm_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    if context.partition_key_range.start == context.partition_key_range.end:
        yield from dlt.run(context=context, dlt_source=filesystem_crm_source(date_partition=context.partition_key))
    else:
        date_list = date_range_list(context.partition_key_range.start, context.partition_key_range.end)
        yield from dlt.run(context=context, dlt_source=filesystem_crm_source(date_partition=date_list))


@dlt_assets(
    dlt_source=filesystem_surveys_source(),
    name="survey_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_surveys_source",  # Needs to match the dlt.source name to avoid state issues
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_surveys",
        destination=dlt.destinations.duckdb(INGEST_SURVEYS_ABS_PATH),
    ),
    partitions_def=daily_partitions,
    group_name="raw_ingestion",
)
def surveys_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    if context.partition_key_range.start == context.partition_key_range.end:
        yield from dlt.run(context=context, dlt_source=filesystem_surveys_source(date_partition=context.partition_key))
    else:
        date_list = date_range_list(context.partition_key_range.start, context.partition_key_range.end)
        yield from dlt.run(context=context, dlt_source=filesystem_surveys_source(date_partition=date_list))


## Discover Asset Keys
# Default conventions seems to be "dlt_<dlt_source_function_name>_<dlt_resource_function_name>"
#
# ASSET KEY: ['dlt_filesystem_calls_source_calls']
# ASSET KEY: ['dlt_filesystem_crm_source_crm']
# ASSET KEY: ['dlt_filesystem_surveys_source_surveys']

# for asset_key in calls_ingestion.keys:
#     print(f"ASSET KEY: {asset_key.path}")
#
# for asset_key in crm_ingestion.keys:
#     print(f"ASSET KEY: {asset_key.path}")
#
# for asset_key in surveys_ingestion.keys:
#     print(f"ASSET KEY: {asset_key.path}")

# TODO: Figure out if any of this should be unit tested

# Testing the dlt code runs locally
# Lesson learned: Running from dagster has a whole different starting location, so we needed to perform a more robust
# absolute pathing structure. In a production deployment, this would likely be a remote location vs. a local one.

# pipeline = dlt.pipeline(
#         pipeline_name="filesystem_calls_source",  # Needs to match the dlt.source name to avoid state issues
#         pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
#         dataset_name="raw_calls",
#         destination=dlt.destinations.duckdb(INGEST_CALLS_ABS_PATH),
# )
#
# load_info = pipeline.run(filesystem_calls_source(date_partition="2025-01-01"))
# print(load_info)

# pipeline =dlt.pipeline(
#         pipeline_name="filesystem_crm_source",
#         pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
#         dataset_name="raw_crm",
#         destination=dlt.destinations.duckdb(INGEST_CRM_ABS_PATH)
# )
#
# load_info = pipeline.run(filesystem_crm_source(date_partition="2025-01-01"))
# print(load_info)
#
# pipeline = dlt.pipeline(
#         pipeline_name="filesystem_surveys_source",
#         pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
#         dataset_name="raw_surveys",
#         destination=dlt.destinations.duckdb(INGEST_SURVEYS_ABS_PATH),
# )
#
# load_info = pipeline.run(filesystem_surveys_source(date_partition="2025-01-01"))
#
# # with pipeline.sql_client() as client:
# #     with client.execute_query("SELECT * FROM raw_calls;") as cursor:
# #         print(cursor.fetchall())
# print(load_info)