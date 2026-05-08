{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'call_month'],
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
  TODO: Implement monthly_call_metrics CTE
  This is the same as daily_agent_metrics but rolled up to month.
  Group by: agent_id, agent_name, cast(date_trunc('month', call_date) as date) as call_month
  Aggregate the same metrics: call_count, customer_resolution_time, avg_hold_time, transfer_rate

  Hint: date_trunc('month', call_date) truncates a date to the first day of its month.
  Cast the result back to date type.
#}

{#
  TODO: Implement monthly_crm_metrics CTE
  Group by: agent_id, agent_name, cast(date_trunc('month', call_date) as date) as call_month
  Aggregate: first_call_resolution (sum of fcr_sum / sum of fcr_denom)
#}

{#
  TODO: Implement monthly_survey_metrics CTE
  Group by: agent_id, agent_name, cast(date_trunc('month', survey_date) as date) as call_month
  Aggregate: survey_count, csat, nps
#}

{#
  TODO: Write the final SELECT joining all three CTEs on agent_id + call_month.
  Same join pattern as daily_agent_metrics — call metrics as base, left join CRM and surveys.

  Output columns:
    agent_id, agent_name, call_month,
    customer_resolution_time, avg_hold_time, transfer_rate,
    first_call_resolution,
    survey_count, csat, nps

  Note: call_count is intentionally omitted at the monthly level (not meaningful to sum across months
  in the same way, but you may add it if you think it adds value — be ready to explain your choice).
#}
