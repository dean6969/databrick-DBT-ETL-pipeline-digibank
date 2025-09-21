{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key([
    'cast(transaction_id as string)',
    "date_format(date_trunc('second', cast(transaction_date as timestamp)), 'yyyyMMddHHmmss')"
  ]) }} as transaction_pk,
    cast(transaction_id      as int)        as transaction_id,
    cast(customer_id         as int)        as customer_id,
    cast(product_id          as int)        as product_id,
    cast(transaction_amount  as double)     as transaction_amount,
    cast(closing_balance     as double)     as closing_balance,
    cast(transaction_date    as timestamp)  as transaction_ts,
    cast(date_trunc('day', cast(transaction_date as timestamp)) as date) as transaction_date,
    cast(date_format(cast(transaction_date as timestamp), 'HH:mm:ss') as string) as transaction_time
from {{ source('bronze', 'raw_transaction_history') }}
