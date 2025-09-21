{% snapshot snp_product_enrollments %}
{{
  config(
    target_database='digibank',
    target_schema='digi_dev_silver',
    unique_key='enrollment_pk',
    strategy='check',
    check_cols=['product_type', 'credit_limit'],   
    invalidate_hard_deletes=true
  )
}}
select
  enrollment_pk,
  product_id         as product_id,
  customer_id,
  enrollment_date,
  upper(trim(product_type))         as product_type,
  credit_limit
from {{ ref('stg_product_enrollments') }}
{% endsnapshot %}
