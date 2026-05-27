{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'manager_id', 'call_date'],
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with call_metrics_src as (
    select *
    from {{ ref('daily_agent_call_metrics_source') }}
    {% if is_incremental() %}
      where call_date  between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

crm_metrics_src as (
    select *
    from {{ ref('daily_agent_crm_metrics_source') }}
    {% if is_incremental() %}
      where call_date  between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

survey_metrics_src as (
    select *
    from {{ ref('daily_agent_survey_metrics_source') }}
    {% if is_incremental() %}
      where survey_date  between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

daily_call_metrics as (
    select
        agent_id
        ,agent_name
        ,manager_id
        ,manager_name
        ,call_date
        ,sum(call_count) as call_count
        ,sum(duration_sum) / sum(call_count) as customer_resolution_time
        ,sum(agent_hold_time_sum) / sum(call_count) as avg_hold_time
        ,sum(transfer_sum) / sum(call_count) as transfer_rate

    from call_metrics_src

    group by
        agent_id,
        agent_name,
        manager_id,
        manager_name,
        call_date
    
),

daily_crm_metrics as (
    select
        agent_id
        ,agent_name
        ,manager_id
        ,manager_name
        ,call_date
        ,sum(fcr_sum) / sum(fcr_denom) as first_call_resolution

    from crm_metrics_src

    group by
        agent_id,
        agent_name,
        manager_id,
        manager_name,
        call_date
),

daily_survey_metrics as (
    select
        agent_id
        ,agent_name
        ,manager_id
        ,manager_name
        ,survey_date as call_date
        ,sum(survey_count) as survey_count
        ,sum(csat_sum) / sum(survey_count) as csat
        ,sum(nps_calc_sum) * 100.0 / sum(survey_count) as nps

    from survey_metrics_src

    group by
        agent_id,
        agent_name,
        manager_id,
        manager_name,
        survey_date
)

select
    d.agent_id
    ,d.agent_name
    ,d.manager_id
    ,d.manager_name
    ,d.call_date
    ,d.call_count
    ,d.customer_resolution_time
    ,d.avg_hold_time
    ,d.transfer_rate
    ,c.first_call_resolution
    ,coalesce(s.survey_count, 0) as survey_count
    ,s.csat
    ,s.nps
    ,NOW() as warehouse_updated_ts

    from daily_call_metrics as d
        left join daily_crm_metrics as c
            on d.agent_id = c.agent_id
            and d.manager_id = c.manager_id
            and d.call_date = c.call_date
        left join daily_survey_metrics as s
            on d.agent_id = s.agent_id
            and d.manager_id = s.manager_id
            and d.call_date = s.call_date
