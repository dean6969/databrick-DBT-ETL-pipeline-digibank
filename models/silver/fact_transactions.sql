{{ config(materialized='table') }}

select
  t.transaction_pk,
  d.date_pk  as transaction_date_pk,
  tm.time_pk as transaction_time_pk,
  t.transaction_id,
  t.customer_id,
  t.product_id,
  t.transaction_amount,
  t.closing_balance,
  date_trunc('second', t.transaction_ts) as transaction_ts
from {{ ref('stg_transaction_history') }} t
left join {{ ref('dim_date') }} d
  on d.date = t.transaction_date
left join {{ ref('dim_time') }} tm
  on tm.time_label = t.transaction_time
-- customer SCD2 
left join {{ ref('dim_customer') }} c
  on c.customer_id = t.customer_id
 and date_trunc('second', t.transaction_ts) >= c.valid_from
 and (c.valid_to is null or date_trunc('second', t.transaction_ts) < c.valid_to)

left join {{ ref('dim_product') }} p
  on p.product_id = t.product_id
 and date_trunc('second', t.transaction_ts) >= p.valid_from
 and (p.valid_to is null or date_trunc('second', t.transaction_ts) < p.valid_to)
;
