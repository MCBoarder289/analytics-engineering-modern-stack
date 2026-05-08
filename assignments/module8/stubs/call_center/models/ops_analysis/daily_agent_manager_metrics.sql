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
  TODO: Implement daily_call_metrics CTE
  This is the same pattern as daily_agent_metrics, but now includes manager dimensions
  because this model supports manager-level reporting.

  Group by: agent_id, agent_name, manager_id, manager_name, call_date
  Aggregate: call_count, customer_resolution_time, avg_hold_time, transfer_rate

  Note: manager_id and manager_name are available in daily_agent_call_metrics_source
  because it already joins to agent_assignments and managers.
#}

{#
  TODO: Implement daily_crm_metrics CTE
  Group by: agent_id, agent_name, manager_id, manager_name, call_date
  Aggregate: first_call_resolution
#}

{#
  TODO: Implement daily_survey_metrics CTE
  Group by: agent_id, agent_name, manager_id, manager_name, survey_date (alias as call_date)
  Aggregate: survey_count, csat, nps
#}

{#
  TODO: Write the final SELECT.
  Join on agent_id + manager_id + call_date (all three keys are needed because an agent can
  theoretically appear under different managers if they changed teams).

  Output columns:
    agent_id, agent_name, manager_id, manager_name, call_date,
    call_count, customer_resolution_time, avg_hold_time, transfer_rate,
    first_call_resolution,
    survey_count, csat, nps
#}
