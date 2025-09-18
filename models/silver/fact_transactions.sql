{{ config(materialized='table') }}

with tx as (
  select
    cast(transaction_id as int)           as transaction_id,
    cast(customer_id as int)              as customer_id,
    cast(product_id  as int)              as product_id,
    cast(transaction_ts as timestamp)     as transaction_ts,
    cast(date_trunc('day', transaction_ts) as date) as transaction_date,
    cast(transaction_amount as double)    as transaction_amount,
    abs(cast(transaction_amount as double)) as transaction_amount_abs,
    cast(closing_balance as double)       as closing_balance
  from {{ ref('stg_transaction_history') }}
  where transaction_id is not null
),
cust_dim as (
  select customer_sk, customer_id,
         valid_from,
         coalesce(valid_to, to_timestamp('2999-12-31')) as valid_to
  from {{ ref('dim_customer_scd2') }}
),
prod_dim as (
  select product_sk, product_id, product_type
  from {{ ref('dim_product') }}
),
enr_scd as (
  select
    customer_id, product_id, product_type,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, to_timestamp('2999-12-31')) as valid_to
  from {{ ref('snp_product_enrollments') }}
),
joined as (
  select
    t.transaction_id,
    t.customer_id,
    cd.customer_sk,
    t.product_id,
    pd.product_sk,
    coalesce(es.product_type, pd.product_type, 'UNKNOWN') as product_type_asof,
    t.transaction_ts,
    t.transaction_date,
    cast(date_format(t.transaction_date,'yyyyMMdd') as int) as date_sk,
    t.transaction_amount,
    t.transaction_amount_abs,
    t.closing_balance
  from tx t
  left join cust_dim cd
    on cd.customer_id = t.customer_id
   and t.transaction_ts >= cd.valid_from
   and t.transaction_ts <  cd.valid_to
  left join prod_dim pd
    on pd.product_id = t.product_id
  left join enr_scd es
    on es.customer_id = t.customer_id
   and es.product_id  = t.product_id
   and t.transaction_ts >= es.valid_from
   and t.transaction_ts <  es.valid_to
)
select * from joined
