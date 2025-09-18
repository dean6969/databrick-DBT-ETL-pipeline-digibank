{{ config(materialized='table') }}

with s as (select * from {{ ref('snp_customer') }})

select
  dbt_scd_id                                        as customer_sk,
  customer_id,
  first_name,
  last_name,
  concat_ws(' ', trim(first_name), trim(last_name)) as full_name,
  email,
  mobile,
  case when upper(gender) in ('M','F') then upper(gender) else 'U' end as gender,
  cast(date_of_birth as date)                       as date_of_birth,
  cast(signup_date as date)                         as signup_date,
  cast(date_trunc('month', signup_date) as date)    as signup_month,
  -- dùng business valid_from để PIT join không bị “tương lai”
  coalesce(to_timestamp(signup_date), dbt_valid_from)                   as valid_from,
  coalesce(dbt_valid_to, to_timestamp('2999-12-31'))                    as valid_to,
  case when dbt_valid_to is null then true else false end               as is_current
from s
