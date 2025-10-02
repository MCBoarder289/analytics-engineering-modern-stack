{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'call_date'],
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with calls as (
    select *
    from {{ ref('mart_calls') }}
    {% if is_incremental() %}
      where start_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

call_agg as (
select
 calls.agent_id
 ,calls.agent_name
 ,cast(calls.start_ts as date) AS call_date
 ,sum(calls.duration_s) as duration_sum
 ,sum(calls.hold_time_during_call_s) as agent_hold_time_sum
 ,sum(case when transfer_flag = TRUE then 1 else 0 end) as transfer_sum
 ,count(1) as call_count

 from calls

 group by 
    calls.agent_id
    ,calls.agent_name
    ,cast(calls.start_ts as date)

)

select
    c.*
    ,a.manager_id
    ,m.manager_name
    ,NOW() as warehouse_updated_ts

    from call_agg as c
    inner join {{ ref('agent_assignments') }} as a
      on c.agent_id = a.agent_id
      and c.call_date between a.effective_start and a.effective_end
    inner join {{ ref('managers') }} as m
      on a.manager_id = m.manager_id