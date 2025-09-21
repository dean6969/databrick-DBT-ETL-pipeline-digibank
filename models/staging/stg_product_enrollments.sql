{{ config(materialized='view') }}

with src as (
  select
      {{ dbt_utils.generate_surrogate_key([
        'product_id',
        'customer_id',
        'product_type'
      ]) }} as enrollment_pk,
      cast(product_id as int)       as product_id,
      cast(customer_id as int)      as customer_id,
      trim(product_type)            as product_type,
      cast(enrollment_date as date) as enrollment_date,
      cast(`limit` as double)       as credit_limit   -- "limit" là từ khóa, đổi tên cho an toàn
  from {{ source('bronze', 'raw_product_enrollments') }}
)

select * from src
