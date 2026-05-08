{{
  config(
    materialized = 'incremental',
    unique_key = ['manager_id', 'call_month'],
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
        manager_id
        ,manager_name
        ,cast(date_trunc('month', call_date) as date) as call_month
        ,sum(call_count) as call_count
        ,sum(duration_sum) / sum(call_count) as customer_resolution_time
        ,sum(agent_hold_time_sum) / sum(call_count) as avg_hold_time
        ,sum(transfer_sum) / sum(call_count) as transfer_rate

    from call_metrics_src

    group by
        manager_id,
        manager_name,
        cast(date_trunc('month', call_date) as date)
    
),

daily_crm_metrics as (
    select
        manager_id
        ,manager_name
        ,cast(date_trunc('month', call_date) as date) as call_month
        ,sum(fcr_sum) / sum(fcr_denom) as first_call_resolution

    from crm_metrics_src

    group by
        manager_id,
        manager_name,
        cast(date_trunc('month', call_date) as date)
),

daily_survey_metrics as (
    select
        manager_id
        ,manager_name
        ,cast(date_trunc('month', survey_date) as date) as call_month
        ,sum(survey_count) as survey_count
        ,sum(csat_sum) / sum(survey_count) as csat
        ,sum(nps_calc_sum) / sum(survey_count) as nps

    from survey_metrics_src

    group by
        manager_id,
        manager_name,
        cast(date_trunc('month', survey_date) as date) 
)

select
    d.manager_id
    ,d.manager_name
    ,d.call_month
    ,d.customer_resolution_time
    ,d.avg_hold_time
    ,d.transfer_rate
    ,c.first_call_resolution
    ,coalesce(s.survey_count, 0) as survey_count
    ,s.csat
    ,s.nps

    from daily_call_metrics as d
        left join daily_crm_metrics as c
            on d.manager_id = c.manager_id
            and d.call_month = c.call_month
        left join daily_survey_metrics as s
            on d.manager_id = s.manager_id
            and d.call_month = s.call_month
