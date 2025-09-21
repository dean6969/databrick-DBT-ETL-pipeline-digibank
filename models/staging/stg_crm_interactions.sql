{{ config(materialized='view') }}

with src as (
  select
      {{ dbt_utils.generate_surrogate_key([
    'cast(interaction_id as string)',
    "cast(row_number() over (
        partition by cast(interaction_id as int)
        order by cast(interaction_date as date), cast(customer_id as int), upper(trim(interaction_type))
      ) as string)"
  ]) }} as interaction_pk,
      cast(interaction_id as int)   as interaction_id,
      cast(customer_id as int)      as customer_id,
      lower(interaction_type)        as interaction_type,
      cast(interaction_date as date) as interaction_date
  from {{ source('bronze', 'raw_crm_interactions') }}
)

select * from src
