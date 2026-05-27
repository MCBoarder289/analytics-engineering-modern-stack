# Module 5 Assignment: Staging Models & Data Deduplication

## Overview

In this assignment you'll build the three **staging models** that form the silver layer of our
call center analytics pipeline. Before you write any SQL, you'll run the pipeline and watch it
fail — because the source data has been intentionally seeded with duplicate records.

Understanding *why* the tests fail and *how to fix it* is the core skill this assignment teaches.

---

## Background

Our ingestion layer (dlt) reads Parquet files from a local filesystem and loads them into DuckDB.
Each time a file is processed, dlt records a `load_id` and an `inserted_at` timestamp in a
metadata table called `_dlt_loads`.

In some pipelines you encounter in the industry, a re-ingestion event (e.g., a pipeline retry or a backfill) can cause 
the same source record to appear in the raw table **more than once** — once for each load that processed it. Each copy has
the same business key (`call_id`, `crm_id`, `survey_id`) but a different `_dlt_load_id`.

Our process actually leverages dlt's incremental cursor, which will ensure that a file is only processed once,
essentially preventing this issue. But that doesn't mean that there isn't bad data within the files themselves.
Often in industry, you'll have duplicates or bad records, which is exactly what we will simulate in this assignment.

The staging models are the right place to resolve this: by keeping only the **most recently
ingested** copy of each record, we produce a clean, deduplicated silver layer.

---

## Setup

Initialize your environment if you haven't already.
From the root of the repo directory, run:
```bash
uv run mds init-env --no-prompt
```

On your branch, you need to set up this scenario by running:
```bash
uv run mds assignment --module 5
```

This:
1. Replaced the staging model files with stub versions (this is your starting point)
2. Copied a handful of source Parquet files under new filenames so dlt will re-ingest them
3. Reset the warehouse and pipeline state so you're starting fresh

---

## Part 1 — Run the pipeline and observe the failures

Spin up Dagster by navigating to the `analytics-system` directory and running `dg dev`:
```bash
cd analytics_system

uv run dg dev
```

1. Open the [Dagster UI](http://127.0.0.1:3000) and run a backfill across all partitions for the three ingestion assets:
   - `dlt_filesystem_calls_source_calls`
   - `dlt_filesystem_crm_source_crm`
   - `dlt_filesystem_surveys_source_surveys`

2. After ingestion completes, run the dbt assets (`seed_data` → `staging_data` → `data_marts`).

3. You should see **unique test failures** on `mart_calls.call_id`, `mart_crm.crm_id`, and
   `mart_surveys.survey_id`. Take a screenshot of the failures.

**Written response (submit with your assignment):**
- What does the `unique` test failure tell you about the data?
- Open the raw DuckDB table and write a query that shows the duplicate records for at least one
  dataset. How many duplicates did you find?

---

## Part 2 — Implement deduplication in the staging models

Each stub file (`stg_calls.sql`, `stg_crm.sql`, `stg_surveys.sql`) has a TODO block describing
what to implement. Your task:

1. Add a `latest_records` CTE that:
   - Joins the `source` CTE to the `_dlt_loads` metadata table on `_dlt_load_id = load_id`
   - Uses `row_number() OVER (PARTITION BY <unique_key> ORDER BY dlt.inserted_at DESC)`
     to rank each duplicate — rank 1 = most recently ingested
   - Selects all business columns plus `NOW() as warehouse_updated_ts`

2. Update the final `SELECT` to read from `latest_records WHERE row_number = 1`

3. Do this for all three staging models.

---

## Part 3 — Verify the fix

Re-run the full pipeline (ingestion → staging → marts → asset checks).

All asset tests should now pass. Take a screenshot of the passing checks.

**Written response:**
- Why do we deduplicate *here* in staging rather than earlier in the ingestion layer or later in
  the mart layer?
- What would happen if two records for the same `call_id` were loaded in the *same* dlt run
  (same `load_id`)? Would your deduplication logic still work?

---

## Deliverables

1. Updated `stg_calls.sql`, `stg_crm.sql`, `stg_surveys.sql` with deduplication implemented
2. Screenshots: failing asset checks (before) and passing asset checks (after)
3. Written responses to the questions in Parts 1 and 3
