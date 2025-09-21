{{ config(materialized='table') }}

{% set lookback_days = var('customer360_lookback_days', 90) %}

with 90_days as (
  -- calculate 90 last until now
  select
    date_sub(current_date(), {{ lookback_days - 1 }}) as start_date,
    current_date()                                    as end_date
),

-- 1) Current customer apply SCD 2
cust as (
  select
    customer_id, first_name, last_name, email, mobile, gender,
    date_of_birth, signup_date
  from {{ ref('dim_customer') }}
  where is_current = true
),

-- 2) total transaction
tx_90d as (
  select
    t.customer_id,
    d.date                 as event_date,
    t.transaction_amount
  from {{ ref('fact_transactions') }} t
  join {{ ref('dim_date') }} d
    on d.date_pk = t.transaction_date_pk
  join 90_days p
    on d.date between p.start_date and p.end_date
  where abs(t.transaction_amount) <> 0
),
tx_agg as (
  select
    customer_id,
    count(*)                as txn_count_90d,
    ROUND(sum(transaction_amount),2) as txn_amount_90d,
    max(event_date)         as last_transaction_date
  from tx_90d
  group by customer_id
),

-- 3) Tương tác trong 90 ngày
intr_90d as (
  select
    fi.customer_id,
    d.date as event_date
  from {{ ref('fact_interactions') }} fi
  join {{ ref('dim_date') }} d
    on d.date_pk = fi.interaction_date_pk
  join 90_days p
    on d.date between p.start_date and p.end_date
),
intr_agg as (
  select
    customer_id,
    count(*)        as interactions_90d,
    max(event_date) as last_interaction_date
  from intr_90d
  group by customer_id
),

-- 4) active product (current holding) + total credit limit
holdings as (
  select
    fpe.customer_id,
    count(*)                                                   as products_held_count,
    concat_ws(', ', collect_set(lower(trim(dp.product_type)))) as product_types_held,
    sum(case when lower(trim(dp.product_type)) = 'credit card'
             then coalesce(fpe.credit_limit, 0.0) else 0.0 end) as total_credit_limit_current
  from {{ ref('fact_product_enrollments') }} fpe
  join {{ ref('dim_product') }} dp
    on dp.product_id = fpe.product_id
   and dp.is_current = true
  group by fpe.customer_id
),

-- 5) Lifetime value
lifetime_tx as (
  select
    customer_id,
    ROUND(sum(transaction_amount)) as total_txn_value_lifetime
  from {{ ref('fact_transactions') }}
  group by customer_id
),

-- 6) summary Customer_360
final as (
  select
    c.customer_id,
    c.first_name, c.last_name, c.email, c.mobile, c.gender,
    c.date_of_birth, c.signup_date,

    case
      when coalesce(tx.txn_count_90d, 0) > 0
        or coalesce(intr.interactions_90d, 0) > 0
      then true else false
    end                                         as is_active90,

    tx.last_transaction_date,
    intr.last_interaction_date,
    coalesce(tx.txn_count_90d, 0)               as txn_count_90d,
    coalesce(tx.txn_amount_90d, 0.0)            as txn_amount_90d,
    coalesce(l.total_txn_value_lifetime, 0.0)   as total_txn_value_lifetime,

    coalesce(h.products_held_count, 0)          as products_held_count,
    h.product_types_held,
    coalesce(h.total_credit_limit_current, 0.0) as total_credit_limit_current
  from cust c
  left join tx_agg     tx   on tx.customer_id   = c.customer_id
  left join intr_agg   intr on intr.customer_id = c.customer_id
  left join holdings   h    on h.customer_id    = c.customer_id
  left join lifetime_tx l   on l.customer_id    = c.customer_id
)

select *
from final
where is_active90 = true;
