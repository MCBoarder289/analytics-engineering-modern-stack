
{{
  config(
    materialized = 'incremental',
    unique_key = 'call_id',
    incremental_strategy='delete_insert',
    tags=["daily"]
    )
}}

with stg_data as (
    select *
    from {{ ref('stg_calls') }}
    {% if is_incremental() %}
      where start_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
)

select
 stg_data.call_id
 ,stg_data.agent_id
 ,agents.agent_name
 ,stg_data.customer_id
 ,customers.program AS customer_program
 ,stg_data.queue_hold_time
 ,stg_data.start_ts
 ,stg_data.end_ts
 ,stg_data.duration_s
 ,stg_data.hold_time_during_call_s
 ,stg_data.transfer_flag
 ,NOW() as warehouse_updated_ts

from stg_data
inner join {{ ref('agents') }} as agents
    on stg_data.agent_id = agents.agent_id
inner join {{ ref('customers') }} as customers
    on stg_data.customer_id = customers.customer_id

