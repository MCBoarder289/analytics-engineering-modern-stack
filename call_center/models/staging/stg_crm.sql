{# Config block at beginning to handle incremental loads #}

{{ config(
    materialized='incremental',
    unique_key='crm_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
) }}

with source as (
    select * 
    from {{ source('ingest_crm', 'crm') }}
    {# In the incremental block, can add a lookback window that will subtract x days fromt the start date #}
    {% if is_incremental() %}
        where created_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
  ),
  {# This is a good example to have students handle de-duplication here vs. in ingest #}
  latest_records as (
      select
        s.crm_id
        ,s.agent_id
        ,s.call_id
        ,s.customer_id
        ,s.reason_code
        ,s.sub_reason_code
        ,s.previous_issue_flag
        ,s.created_ts
        ,row_number() over (partition by s.crm_id order by dlt.inserted_at desc) as row_number

      from source s

        inner join (
            select
                load_id
                ,inserted_at
            from {{ source('ingest_crm', '_dlt_loads') }}
        ) dlt
            on s._dlt_load_id = dlt.load_id
  )
  
select
    crm_id
    ,agent_id
    ,call_id
    ,customer_id
    ,reason_code
    ,sub_reason_code
    ,previous_issue_flag
    ,created_ts

    from latest_records

    where
        row_number == 1
    