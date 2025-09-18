{{ config(materialized='table') }}

with src as (
  select
      cast(product_id          as int)        as product_id,
      cast(customer_id         as int)        as customer_id,
      cast(transaction_amount  as double)     as transaction_amount,
      cast(closing_balance     as double)     as closing_balance,
      cast(transaction_date    as timestamp)  as transaction_ts,
      cast(transaction_id      as int)        as transaction_id
  from {{ source('bronze', 'raw_transaction_history') }}
),

final as (
  select
      transaction_id,
      customer_id,
      product_id,
      transaction_amount,
      closing_balance,
      transaction_ts,
      cast(date_trunc('day', transaction_ts) as date) as transaction_date
  from src
)

select * from final
