
{{
  config(
    materialized = 'incremental',
    unique_key = 'survey_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with stg_data as (
    select *
    from {{ ref('stg_surveys') }}
    {% if is_incremental() %}
      where response_ts between '{{ var("start_date") }}' and '{{ var("end_date") }}' 
    {% endif %}
)

select
 stg_data.survey_id
 ,stg_data.call_id
 ,stg_data.agent_id
 ,agents.agent_name
 ,stg_data.customer_id
 ,customers.program AS customer_program
 ,stg_data.sent_ts
 ,stg_data.response_ts
 ,DATE_DIFF('day', stg_data.sent_ts, stg_data.response_ts) AS days_to_respond
 ,stg_data.csat
 ,stg_data.nps

 {#
   TODO: Calculate nps_calc using a CASE expression on the raw `nps` score (0–10).

   Business context:
     Net Promoter Score (NPS) measures how likely a customer is to recommend our service.
     After each call, customers answer: "On a scale of 0–10, how likely are you to recommend
     this service to a friend or colleague?"

     We classify responses into three groups:
       - Detractors  (score 0–6):  Unhappy customers who can hurt our brand. Value: -1
       - Passives    (score 7–8):  Satisfied but unenthusiastic customers.   Value:  0
       - Promoters   (score 9–10): Loyal enthusiasts who will refer others.  Value: +1

     The nps_calc column should return -1, 0, or 1 according to these thresholds.
     This value is later summed and averaged across agents to produce an NPS score.

   Name the calculated field: nps_calc
 #}

 ,NOW() as warehouse_updated_ts

from stg_data
inner join {{ ref('agents') }} as agents
    on stg_data.agent_id = agents.agent_id
inner join {{ ref('customers') }} as customers
    on stg_data.customer_id = customers.customer_id
