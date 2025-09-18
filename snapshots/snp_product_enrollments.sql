{% snapshot snp_product_enrollments %}
{{
  config(
    target_database='digibank',
    target_schema='digi-dev-silver',
    unique_key='customer_id || "-" || product_id',
    strategy='check',
    check_cols=['product_type','credit_limit']
  )
}}
select
  customer_id,
  product_id,
  upper(trim(product_type))        as product_type,
  cast(enrollment_date as date)    as enrollment_date,
  cast(credit_limit as double)     as credit_limit
from {{ ref('stg_product_enrollments') }}
{% endsnapshot %}
