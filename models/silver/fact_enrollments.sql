{{ config(materialized='table') }}

with base as (
  select
    cast(customer_id as int)      as customer_id,
    cast(product_id as int)       as product_id,
    upper(trim(product_type))     as product_type,
    cast(enrollment_date as date) as enrollment_date,
    cast(credit_limit as double)  as credit_limit
  from {{ ref('stg_product_enrollments') }}
),
cust_dim as (
  select customer_sk, customer_id,
         valid_from,
         coalesce(valid_to, to_timestamp('2999-12-31')) as valid_to
  from {{ ref('dim_customer_scd2') }}
),
prod_dim as (
  select product_sk, product_id, product_type from {{ ref('dim_product') }}
),
joined as (
  select
    b.customer_id,
    cd.customer_sk,
    b.product_id,
    pd.product_sk,
    b.product_type,
    b.enrollment_date,
    cast(date_format(b.enrollment_date,'yyyyMMdd') as int) as date_sk,
    b.credit_limit
  from base b
  left join cust_dim cd
    on cd.customer_id = b.customer_id
   and to_timestamp(b.enrollment_date) >= cd.valid_from
   and to_timestamp(b.enrollment_date) <  cd.valid_to
  left join prod_dim pd
    on pd.product_id = b.product_id
)
select * from joined
