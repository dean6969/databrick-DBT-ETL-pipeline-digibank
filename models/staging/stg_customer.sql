{{ config(materialized='view') }}

with src as (
    select
        cast(customer_id as int)              as customer_id,
        trim(first_name)                      as first_name,
        trim(last_name)                       as last_name,
        lower(trim(email))                    as email,
        nullif(regexp_replace(trim(mobile), '[^0-9+]', ''), '') as raw_mobile,
        trim(gender)                          as gender,
        cast(date_of_birth as date)           as date_of_birth,
        cast(signup_date as date)             as signup_date
    from {{ source('bronze', 'raw_customer_raw') }}
),

cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        email,

        -- chuẩn hoá mobile:
        case
            when raw_mobile like '+63%' then regexp_replace(raw_mobile, '^\+63', '0')
            when raw_mobile like '63%'  then regexp_replace(raw_mobile, '^63', '0')
            when raw_mobile like '0%'   then raw_mobile
            else raw_mobile -- fallback: keep original pattern if not match above pattern
        end as mobile,

        lower(gender) as gender,
        date_of_birth,
        signup_date
    from src
)

select * from cleaned
