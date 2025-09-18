{{ config(materialized='table') }}

with src as (
  select
      cast(interaction_id as int)   as interaction_id,
      cast(customer_id as int)      as customer_id,
      trim(interaction_type)        as interaction_type,
      cast(interaction_date as date) as interaction_date
  from {{ source('bronze', 'raw_crm_interactions') }}
)

select * from src
