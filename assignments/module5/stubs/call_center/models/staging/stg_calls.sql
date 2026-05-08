{# Config block at beginning to handle incremental loads #}

{{ 
    config(
    materialized='incremental',
    unique_key='call_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    ) 
}}

with source as (
    select * 
    from {{ source('ingest_calls', 'calls') }}
    {# In the incremental block, can add a lookback window that will subtract x days from the start date #}
    {% if is_incremental() %}
        where start_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
  )

{#
  TODO: The data warehouse has received duplicate records due to an upstream re-ingestion event.
  Multiple rows can exist for the same call_id with different dlt load timestamps.

  Your task: Implement a deduplication CTE called `latest_records` that:
    1. Joins the source CTE to the dlt loads table:
         {{ source('ingest_calls', '_dlt_loads') }}
       matching on s._dlt_load_id = dlt.load_id to get the inserted_at timestamp.
    2. Uses a window function (row_number) partitioned by call_id, ordered by
       dlt.inserted_at DESC so that the most recently ingested record ranks first.
    3. Selects these columns from the source:
         call_id, agent_id, customer_id, queue_hold_time, start_ts, end_ts,
         duration_s, hold_time_during_call_s, transfer_flag
       Plus NOW() as warehouse_updated_ts.
    4. In the final SELECT, filters to only row_number = 1.

  Hint: Without deduplication, the unique test on call_id in mart_calls will fail.
  You can observe this failure in the Dagster asset checks after running the pipeline.
#}

select
    call_id
    ,agent_id
    ,customer_id
    ,queue_hold_time
    ,start_ts
    ,end_ts
    ,duration_s
    ,hold_time_during_call_s
    ,transfer_flag
    ,NOW() as warehouse_updated_ts

    from source
