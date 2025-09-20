{# Config block at beginning to handle incremental loads #}

{{ config(
    materialized='incremental',
    unique_key='call_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
) }}

with source as (
    select * 
    from {{ source('ingest_surveys', 'surveys') }}
    {# In the incremental block, can add a lookback window that will subtract x days fromt the start date #}
    {% if is_incremental() %}
        where response_ts between '{{ var("min_date") }}' and '{{ var("max_date") }}' 
    {% endif %}
  ),
  {# This is a good example to have students handle de-duplication here vs. in ingest #}
  latest_records as (
      select
        s.survey_id
        ,s.call_id
        ,s.agent_id
        ,s.customer_id
        ,s.sent_ts
        ,s.response_ts
        ,s.csat
        ,s.nps
        ,row_number() over (partition by s.survey_id order by dlt.inserted_at desc) as row_number

      from source s

        inner join (
            select
                load_id
                ,inserted_at
            from {{ source('ingest_surveys', '_dlt_loads') }}
        ) dlt
            on s._dlt_load_id = dlt.load_id
  )
  
  select
    survey_id
    ,call_id
    ,agent_id
    ,customer_id
    ,sent_ts
    ,response_ts
    ,csat
    ,nps

    from latest_records

    where
        row_number == 1
    