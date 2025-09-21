{{ config(materialized='table') }}

select
  enrollment_pk,
  product_id     as product_id,
  product_type,
  dbt_valid_from                as valid_from,
  dbt_valid_to                  as valid_to,
  case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snp_product_enrollments') }}
