{{
  config(
    materialized = 'incremental',
    unique_key = ['agent_id', 'survey_date'],
    incremental_strategy='delete+insert',
    tags=['daily']
    )
}}

with surveys as (
    select *
    from {{ ref('mart_surveys') }}
    {% if is_incremental() %}
    {# Need to implement a lookback period for late-arriving surveys on ingestion date #}
      where cast(sent_ts as date) between date_add('day', -{{ var('lookback_days', 7) }},'{{ var("start_date") }}') and '{{ var("end_date") }}'
    {% endif %}
),

surveys_agg as (
    select
     surveys.agent_id
     ,surveys.agent_name
     ,cast(surveys.sent_ts as date) AS survey_date
     ,sum(surveys.csat) as csat_sum
     ,sum(nps_calc) as nps_calc_sum
     ,count(1) as survey_count

    from surveys

    group by
        surveys.agent_id
        ,surveys.agent_name
        ,cast(surveys.sent_ts as date)

)

select
    s.*
    ,a.manager_id
    ,m.manager_name
    ,NOW() as warehouse_updated_ts

    from surveys_agg as s
    inner join {{ ref('agent_assignments') }} as a
        on s.agent_id = a.agent_id
        and s.survey_date between a.effective_start and a.effective_end
    inner join {{ ref('managers') }} as m
        on a.manager_id = m.manager_id