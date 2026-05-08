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
  ),
  {# This is a good example to have students handle de-duplication here vs. in ingest #}
  latest_records as (
      select
        s.call_id
        ,s.agent_id
        ,s.customer_id
        ,s.queue_hold_time
        ,s.start_ts
        ,s.end_ts
        ,s.duration_s
        ,s.hold_time_during_call_s
        ,s.transfer_flag
        ,row_number() over (partition by s.call_id order by dlt.inserted_at desc) as row_number
        ,NOW() as warehouse_updated_ts

      from source s

        inner join (
            select
                load_id
                ,inserted_at
            from {{ source('ingest_calls', '_dlt_loads') }}
        ) dlt
            on s._dlt_load_id = dlt.load_id
  )
  
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
    ,warehouse_updated_ts
    
    from latest_records

    where
        row_number == 1
    