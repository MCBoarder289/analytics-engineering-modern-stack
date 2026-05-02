{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'month'],
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with call_metrics as (
    select *
    from {{ ref('daily_agent_call_metrics_source') }}
    {% if is_incremental() %}
      where call_date  between '{{ var("start_date") }}' and '{{ var("end_date") }}'
    {% endif %}
)

select
    manager_id
    ,manager_name
    ,cast(date_trunc('month', call_date) as date) as month
    ,sum(call_count) as call_count
    ,sum(duration_sum) / sum(call_count) as customer_resolution_time
    ,sum(agent_hold_time_sum) / sum(call_count) as avg_hold_time
    ,sum(transfer_sum) / sum(call_count) as transfer_rate

    from call_metrics

    group by
        manager_id,
        manager_name,
        cast(date_trunc('month', call_date) as date)
