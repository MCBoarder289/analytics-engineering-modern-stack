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
  TODO: Implement the three aggregation CTEs rolled up to manager + month.
  This model collapses the agent dimension entirely — it reports purely at the manager level.

  Group by: manager_id, manager_name,
            cast(date_trunc('month', call_date) as date) as call_month

  Aggregate the same metrics as the other monthly models.
  Think about what changes when you remove agent_id from the group-by:
    - How does this affect transfer_rate and FCR calculations?
    - Does it still make sense to divide by call_count? Why?
#}

{#
  TODO: Write the final SELECT joining all three CTEs on manager_id + call_month.

  Output columns:
    manager_id, manager_name, call_month,
    customer_resolution_time, avg_hold_time, transfer_rate,
    first_call_resolution,
    survey_count, csat, nps
#}
