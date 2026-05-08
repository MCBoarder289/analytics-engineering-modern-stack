{{
  config(
    materialized = 'incremental',
    unique_key = 'crm_id',
    incremental_strategy='delete+insert',
    tags=["daily"]
    )
}}

with disqualifying_callbacks as (
    select distinct original_crm_id
    from {{ ref('mart_crm_callbacks') }}
    where same_reason_flag
        and same_sub_reason_flag
        {#
          TODO (Part 2): Agents now record whether a callback is about a previous issue via the
          `previous_issue_flag` field in the CRM system. This value flows through to
          `mart_crm_callbacks` as `callback_previous_issue_flag`.

          The business has decided that if an agent flags a callback as being about a previous
          issue, that callback should ALSO disqualify the original call from first call resolution
          — regardless of whether the reason codes match.

          Add `callback_previous_issue_flag` as an additional disqualifying condition here.

          After making this change, re-run the FCR assets and compare the resulting
          first_call_resolution rates in daily_agent_metrics vs. before your change.
          In your written response, describe:
            1. Did FCR go up or down? Why?
            2. Is this a more accurate measurement of FCR? Why or why not?
        #}
        {% if is_incremental() %}
          and cast(original_created_ts as date) 
          between DATE '{{ var("start_date") }}' - INTERVAL '{{ var("lookback_days", 7) }} day'
          and DATE '{{ var("end_date") }}'
        {% endif %}
)

select
    crm.crm_id
    ,crm.agent_id
    ,crm.agent_name
    ,cast(crm.created_ts as date) as fcr_date
    ,1 as fcr_denom
    ,case
        when dc.original_crm_id is null then 1
        else 0
        end as fcr_num
    from {{ ref('mart_crm') }} crm

    left join disqualifying_callbacks dc
        on crm.crm_id = dc.original_crm_id

    {% if is_incremental() %}
      where
        cast(crm.created_ts as date) 
         between DATE '{{ var("start_date") }}' - INTERVAL '{{ var("lookback_days", 7) }} day'
          and DATE '{{ var("end_date") }}'
    {% endif %}
