{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'call_date'],
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
  Group by: agent_id, agent_name, call_date
  Aggregate:
    - call_count:               total number of calls (sum of call_count)
    - customer_resolution_time: average call duration in seconds (sum of duration_sum / sum of call_count)
    - avg_hold_time:            average hold time per call (sum of agent_hold_time_sum / sum of call_count)
    - transfer_rate:            fraction of calls transferred (sum of transfer_sum / sum of call_count)
#}

{#
  TODO: Implement daily_crm_metrics CTE
  Group by: agent_id, agent_name, call_date
  Aggregate:
    - first_call_resolution: FCR rate (sum of fcr_sum / sum of fcr_denom)
#}

{#
  TODO: Implement daily_survey_metrics CTE
  Group by: agent_id, agent_name, survey_date (alias as call_date in final SELECT)
  Aggregate:
    - survey_count: total number of survey responses (count)
    - csat:         average CSAT score (sum of csat_sum / sum of survey_count)
    - nps:          average NPS calc value (sum of nps_calc_sum / sum of survey_count)
#}

{#
  TODO: Write the final SELECT joining all three CTEs.
  Use daily_call_metrics as the base (every agent with calls gets a row).
  Left join daily_crm_metrics and daily_survey_metrics on agent_id + call_date.
  Use coalesce(s.survey_count, 0) for survey_count since not all agents receive surveys every day.

  Output columns:
    agent_id, agent_name, call_date,
    call_count, customer_resolution_time, avg_hold_time, transfer_rate,
    first_call_resolution,
    survey_count, csat, nps
#}
