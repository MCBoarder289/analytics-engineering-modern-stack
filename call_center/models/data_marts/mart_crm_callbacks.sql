{{
  config(
    materialized = 'incremental',
    unique_key = ['original_crm_id', 'callback_crm_id'],
    incremental_strategy = 'delete+insert',
    tags=['daily']
    )
}}

select
  crm1.crm_id as original_crm_id
  ,crm2.crm_id as callback_crm_id
  ,crm1.agent_id as original_agent_id
  ,crm2.agent_id as callback_agent_id
  ,crm1.customer_id
  ,crm1.created_ts as original_created_ts
  ,crm2.created_ts as callback_created_ts
  ,datediff('day', crm1.created_ts, crm2.created_ts) as days_between
  ,crm1.reason_code = crm2.reason_code as same_reason_flag
  ,crm1.sub_reason_code = crm2.sub_reason_code as same_sub_reason_flag
  ,crm2.previous_issue_flag as callback_previous_issue_flag
  ,NOW() as warehouse_updated_ts
from {{ ref('mart_crm') }} crm1
inner join {{ ref('mart_crm') }} crm2
  on crm1.customer_id = crm2.customer_id
  and crm2.created_ts > crm1.created_ts
  and crm2.created_ts <= crm1.created_ts + interval '3 days'

{% if is_incremental() %}
where 
  crm2.created_ts < '{{ var("end_date") }}'
  {# Adding a timebox for crm1 to make backfills more efficient - not scanning the entire crm table to join to #}
  and cast(crm1.created_ts as date) >= CAST('{{ var("start_date") }}' AS DATE) - INTERVAL '{{ var("lookback_days", 7) }} day'
{% endif %}

