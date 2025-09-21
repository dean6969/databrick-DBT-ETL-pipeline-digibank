{{ config(materialized='table') }}

select 
  interaction_pk,
  interaction_id,
  interaction_type
from {{ ref('stg_crm_interactions') }}
where interaction_type is not null
  and trim(interaction_type) <> ''
