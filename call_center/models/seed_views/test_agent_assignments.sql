
{{
  config(
    materialized = 'view',
    )
}}

-- depends_on: {{ ref('agents') }}
-- depends_on: {{ ref('managers') }}


select * from {{ ref('agent_assignments') }}