import os
from collections.abc import Callable, Generator
from datetime import date, timedelta
from typing import Any, cast

import dlt
from dagster import AssetExecutionContext, AssetMaterialization, MaterializeResult
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt.extract import DltResource
from dlt.sources.filesystem import filesystem, read_parquet

from analytics_system.constants import (
    DAILY_PARTITION,
    DLT_STATE_LOCATION_ABS_PATH,
    INGEST_CALLS_ABS_PATH,
    INGEST_CRM_ABS_PATH,
    INGEST_SURVEYS_ABS_PATH,
    SOURCE_DATA_DIR_PATH,
)

dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True  # TODO: Figure out why this is needed vs. config.toml


def parquet_day_partition(dataset: str, date_partition: str) -> DltResource:
    """Returns a dlt resource that reads all parquet files for a single day partition.

    Uses incremental loading by file_url (ascending) to make runs idempotent. This assumes
    partitions are backfilled sequentially in chronological order — retroactive backfills on
    already-processed date ranges will be skipped by the cursor state.
    """
    abs_path = os.path.abspath(os.path.join(SOURCE_DATA_DIR_PATH, f"{dataset}/day={date_partition}/"))
    fs = filesystem(
        f"file://{abs_path}",
        file_glob="*.parquet",
        incremental=dlt.sources.incremental("file_url", row_order="asc"),
    )
    # dlt supports piping filesystem() into read_parquet() via __or__, but the return type
    # is not reflected in dlt's stubs — cast makes the intent explicit without suppressing
    # unrelated type errors on the same line.
    return cast(DltResource, fs | read_parquet(use_pyarrow=True))


def date_range_list(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def make_filesystem_source(dataset: str) -> Callable[..., Any]:
    """Factory that produces a named dlt source for a local parquet dataset.

    The source name (e.g. ``filesystem_calls_source``) must match the dlt pipeline's
    ``pipeline_name`` to avoid state key mismatches across runs.
    """

    @dlt.source(name=f"filesystem_{dataset}_source")
    def _source(date_partition: str | list[str] | None = None):
        @dlt.resource(name=dataset)
        def _resource():
            # Called with no arguments by @dlt_assets at definition time for schema introspection.
            if date_partition is None:
                return
            if isinstance(date_partition, str):
                yield from parquet_day_partition(dataset=dataset, date_partition=date_partition)
            else:
                for d in date_partition:
                    yield from parquet_day_partition(dataset=dataset, date_partition=d)

        return _resource

    return _source


filesystem_calls_source = make_filesystem_source("calls")
filesystem_crm_source = make_filesystem_source("crm")
filesystem_surveys_source = make_filesystem_source("surveys")


def _run_partitioned(
    context: AssetExecutionContext,
    dlt_resource: DagsterDltResource,
    source_fn: Callable,
) -> Generator[AssetMaterialization | MaterializeResult, None, None]:
    """Runs a partitioned dlt source, handling both single-partition and range materializations.

    For individual partition runs (the normal backfill case), ``rows_loaded`` in the Dagster
    metadata accurately reflects that partition's row count. For range materializations (multiple
    partitions selected at once in the UI), all dates are batched into a single dlt pipeline run,
    so ``rows_loaded`` is the aggregate total across the entire range — not broken down per date.
    """
    start, end = context.partition_key_range.start, context.partition_key_range.end
    date_partition = context.partition_key if start == end else date_range_list(start, end)
    yield from dlt_resource.run(context=context, dlt_source=source_fn(date_partition=date_partition))


@dlt_assets(
    dlt_source=filesystem_calls_source(),
    name="calls_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_calls_source",  # Must match the dlt.source name to avoid state key mismatches
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_calls",
        destination=dlt.destinations.duckdb(INGEST_CALLS_ABS_PATH),
    ),
    partitions_def=DAILY_PARTITION,
    group_name="raw_ingestion",
)
def calls_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    # Parameter named 'dlt' to match the Dagster resource key — shadowing of the dlt module is
    # intentional here; the module is not referenced inside this function body.
    yield from _run_partitioned(context, dlt, filesystem_calls_source)


@dlt_assets(
    dlt_source=filesystem_crm_source(),
    name="crm_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_crm_source",  # Must match the dlt.source name to avoid state key mismatches
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_crm",
        destination=dlt.destinations.duckdb(INGEST_CRM_ABS_PATH),
    ),
    partitions_def=DAILY_PARTITION,
    group_name="raw_ingestion",
)
def crm_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from _run_partitioned(context, dlt, filesystem_crm_source)


@dlt_assets(
    dlt_source=filesystem_surveys_source(),
    name="survey_ingestion_assets",
    dlt_pipeline=dlt.pipeline(
        pipeline_name="filesystem_surveys_source",  # Must match the dlt.source name to avoid state key mismatches
        pipelines_dir=DLT_STATE_LOCATION_ABS_PATH,
        dataset_name="raw_surveys",
        destination=dlt.destinations.duckdb(INGEST_SURVEYS_ABS_PATH),
    ),
    partitions_def=DAILY_PARTITION,
    group_name="raw_ingestion",
)
def surveys_ingestion(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from _run_partitioned(context, dlt, filesystem_surveys_source)


# Asset key convention: "dlt_<source_name>_<resource_name>"
#   dlt_filesystem_calls_source_calls
#   dlt_filesystem_crm_source_crm
#   dlt_filesystem_surveys_source_surveys
