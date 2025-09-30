
{{
  config(
    materialized = 'incremental',
    unique_key = 'survey_id',
    incremental_strategy='delete_insert',
    tags=["daily"]
    )
}}

with stg_data as (
    select *
    from {{ ref('stg_surveys') }}
    {% if is_incremental() %}
      where response_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
)

select
 stg_data.survey_id
 ,stg_data.call_id
 ,stg_data.agent_id
 ,agents.agent_name
 ,stg_data.customer_id
 ,customers.program AS customer_program
 ,stg_data.sent_ts
 ,stg_data.response_ts
 ,DATE_DIFF('day', stg_data.sent_ts, stg_data.response_ts) AS days_to_respond
 ,stg_data.csat
 ,stg_data.nps
 ,case 
    when stg_data.nps <= 6 then -1
    when stg_data.nps between 7 and 8 then 0
    when stg_data.nps >= 9 then 1
    end AS nps_calc 
 ,NOW() as warehouse_updated_ts

from stg_data
inner join {{ ref('agents') }} as agents
    on stg_data.agent_id = agents.agent_id
inner join {{ ref('customers') }} as customers
    on stg_data.customer_id = customers.customer_id

