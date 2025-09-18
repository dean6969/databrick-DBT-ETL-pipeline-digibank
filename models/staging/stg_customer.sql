{{ config(materialized='table') }}

with src as (
  select
      cast(customer_id as int)              as customer_id,
      trim(first_name)                      as first_name,
      trim(last_name)                       as last_name,
      lower(trim(email))                    as email,
      regexp_replace(trim(mobile), '\s+', '') as mobile,
      trim(gender)                          as gender,
      cast(date_of_birth as date)           as date_of_birth,
      cast(signup_date as date)             as signup_date
  from {{ source('bronze', 'raw_customer_raw') }}
),

final as (
  select
      customer_id,
      first_name,
      last_name,
      concat_ws(' ', first_name, last_name) as full_name,
      email,
      mobile,
      gender,
      date_of_birth,
      signup_date
  from src
)

select * from final
