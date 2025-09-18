{{ config(materialized='table') }}

with base as (
  select distinct
    cast(product_id as int)               as product_id,
    upper(trim(product_type))             as product_type
  from {{ ref('stg_product_enrollments') }}
)
select
  md5(cast(product_id as string))         as product_sk,
  product_id,
  product_type
from base
