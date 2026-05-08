{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'manager_id', 'call_month'],
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with call_metrics_src as (
    select *
    from {{ ref('daily_agent_call_metrics_source') }}
    {% if is_incremental() %}
      where call_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

crm_metrics_src as (
    select *
    from {{ ref('daily_agent_crm_metrics_source') }}
    {% if is_incremental() %}
      where call_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

survey_metrics_src as (
    select *
    from {{ ref('daily_agent_survey_metrics_source') }}
    {% if is_incremental() %}
      where survey_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
),

{#
  TODO: Implement the three aggregation CTEs (call, crm, survey) rolled up to month.
  Group by: agent_id, agent_name, manager_id, manager_name,
            cast(date_trunc('month', call_date) as date) as call_month

  Same metrics as daily_agent_manager_metrics but at monthly granularity.
#}

{#
  TODO: Write the final SELECT joining all three CTEs on agent_id + manager_id + call_month.

  Output columns:
    agent_id, agent_name, manager_id, manager_name, call_month,
    customer_resolution_time, avg_hold_time, transfer_rate,
    first_call_resolution,
    survey_count, csat, nps
#}
