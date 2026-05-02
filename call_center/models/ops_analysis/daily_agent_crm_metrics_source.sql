{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'call_date'],
    incremental_strategy='delete+insert',
    tags=['daily']
    )
}}

with fcr as (
    select *
    from {{ ref('mart_first_call_resolution') }}
    {% if is_incremental() %}
    {# Need to implement a lookback period because FCR develops over time #}
      where cast(fcr_date as date) between CAST('{{ var("start_date") }}' AS DATE) - INTERVAL '{{ var("lookback_days", 7) }} day' and CAST('{{ var("end_date") }}' AS DATE)
    {% endif %}
),

fcr_agg as (
    select
        fcr.agent_id
        ,fcr.agent_name
        ,fcr.fcr_date as call_date
        ,sum(fcr.fcr_num) as fcr_sum
        ,sum(fcr.fcr_denom) as fcr_denom
        
        from fcr

        group by
            fcr.agent_id
            ,fcr.agent_name
            ,fcr.fcr_date

)

select
    f.*
    ,a.manager_id
    ,m.manager_name
    ,NOW() as warehouse_updated_ts

    from fcr_agg as f
    inner join {{ ref('agent_assignments') }} as a
        on f.agent_id = a.agent_id
        and f.call_date between a.effective_start and a.effective_end
    inner join {{ ref('managers') }} as m
        on a.manager_id = m.manager_id
