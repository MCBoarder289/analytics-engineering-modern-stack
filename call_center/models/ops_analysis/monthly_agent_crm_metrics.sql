{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'month'],
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with crm_metrics as (
    select *
    from {{ ref('daily_agent_crm_metrics_source') }}
    {% if is_incremental() %}
      where call_date  between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
)

select
    agent_id
    ,agent_name
    ,cast(date_trunc('month', call_date) as date) as month
    ,sum(fcr_sum) / sum(fcr_denom) as first_call_resolution

    from crm_metrics

    group by
        agent_id,
        agent_name,
        cast(date_trunc('month', call_date) as date)
