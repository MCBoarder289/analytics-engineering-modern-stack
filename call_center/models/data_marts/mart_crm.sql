
{{
  config(
    materialized = 'incremental',
    unique_key = 'crm_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with stg_data as (
    select *
    from {{ ref('stg_crm') }}
    {% if is_incremental() %}
      where created_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
)

select
 stg_data.crm_id
 ,stg_data.agent_id
 ,agents.agent_name
 ,stg_data.call_id
 ,stg_data.customer_id
 ,customers.program AS customer_program
 ,stg_data.reason_code
 ,stg_data.sub_reason_code
 ,stg_data.previous_issue_flag
 ,stg_data.created_ts
 ,NOW() as warehouse_updated_ts

from stg_data
inner join {{ ref('agents') }} as agents
    on stg_data.agent_id = agents.agent_id
inner join {{ ref('customers') }} as customers
    on stg_data.customer_id = customers.customer_id

