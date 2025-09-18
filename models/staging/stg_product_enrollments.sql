{{ config(materialized='table') }}

with src as (
  select
      cast(product_id as int)       as product_id,
      cast(customer_id as int)      as customer_id,
      trim(product_type)            as product_type,
      cast(enrollment_date as date) as enrollment_date,
      cast(`limit` as double)       as credit_limit   -- "limit" là từ khóa, đổi tên cho an toàn
  from {{ source('bronze', 'raw_product_enrollments') }}
)

select * from src
