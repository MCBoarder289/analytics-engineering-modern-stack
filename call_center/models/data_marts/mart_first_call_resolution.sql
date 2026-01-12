{{
  config(
    materialized = 'incremental',
    unique_key = 'crm_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with disqualifying_callbacks as (
    select distinct original_crm_id
    from {{ ref('mart_crm_callbacks') }}
    where same_reason_flag
        and same_sub_reason_flag
        {% if is_incremental() %}
          and cast(original_created_ts as date) 
          between DATE '{{ var("start_date") }}' - INTERVAL '{{ var("lookback_days", 7) }} day'
          and DATE '{{ var("end_date") }}'
        {% endif %}
)

select
    crm.crm_id
    ,crm.agent_id
    ,crm.agent_name
    ,cast(crm.created_ts as date) as fcr_date
    ,1 as fcr_denom
    ,case
        when dc.original_crm_id is null then 1
        else 0
        end as fcr_num
    from {{ ref('mart_crm') }} crm

    left join disqualifying_callbacks dc
        on crm.crm_id = dc.original_crm_id

    {% if is_incremental() %}
      where
        cast(crm.created_ts as date) 
         between DATE '{{ var("start_date") }}' - INTERVAL '{{ var("lookback_days", 7) }} day'
          and DATE '{{ var("end_date") }}'
    {% endif %}
