{# Config block at beginning to handle incremental loads #}

{{ 
    config(
    materialized='incremental',
    unique_key='survey_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    ) 
}}

with source as (
    select * 
    from {{ source('ingest_surveys', 'surveys') }}
    {% if is_incremental() %}
        where response_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
  )

{#
  TODO: The data warehouse has received duplicate records due to an upstream re-ingestion event.
  Multiple rows can exist for the same survey_id with different dlt load timestamps.

  Your task: Implement a deduplication CTE called `latest_records` that:
    1. Joins the source CTE to the dlt loads table:
         {{ source('ingest_surveys', '_dlt_loads') }}
       matching on s._dlt_load_id = dlt.load_id to get the inserted_at timestamp.
    2. Uses a window function (row_number) partitioned by survey_id, ordered by
       dlt.inserted_at DESC so that the most recently ingested record ranks first.
    3. Selects these columns from the source:
         survey_id, call_id, agent_id, customer_id, sent_ts, response_ts, csat, nps
       Plus NOW() as warehouse_updated_ts.
    4. In the final SELECT, filters to only row_number = 1.

  Hint: Without deduplication, the unique test on survey_id in mart_surveys will fail.
  You can observe this failure in the Dagster asset checks after running the pipeline.
#}

select
    survey_id
    ,call_id
    ,agent_id
    ,customer_id
    ,sent_ts
    ,response_ts
    ,csat
    ,nps
    ,NOW() as warehouse_updated_ts

    from source
