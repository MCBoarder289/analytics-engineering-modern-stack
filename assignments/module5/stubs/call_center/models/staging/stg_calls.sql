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

  Your task: Implement a deduplication CTE called `latest_records` that resolves this.
  Each call_id should appear exactly once in the output, retaining only the most recently
  ingested version of the record.

  To determine recency, you will need to join the source CTE to the dlt loads table:
    {{ source('ingest_calls', '_dlt_loads') }}
  on s._dlt_load_id = dlt.load_id — this gives you the inserted_at timestamp for each load.

  The final CTE must produce these columns:
    call_id, agent_id, customer_id, queue_hold_time, start_ts, end_ts,
    duration_s, hold_time_during_call_s, transfer_flag, NOW() as warehouse_updated_ts

  Hint: Without deduplication, the unique test on call_id in mart_calls will fail.
  You can observe this failure in the Dagster asset checks after running the pipeline.

  Extra Hint: Think of a windowing function that will help you deduplicate the source...
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
