{% snapshot snp_customer %}
{{
  config(
    target_database='digibank',
    target_schema='digi-dev-silver',
    unique_key='customer_id',
    strategy='check',
    check_cols=['first_name','last_name','email','mobile','gender','date_of_birth','signup_date']
  )
}}
select
  customer_id,
  first_name,
  last_name,
  lower(trim(email))                           as email,
  regexp_replace(trim(mobile),'[^0-9]+','')    as mobile,
  upper(trim(gender))                          as gender,
  cast(date_of_birth as date)                  as date_of_birth,
  cast(signup_date as date)                    as signup_date
from {{ ref('stg_customer') }}
{% endsnapshot %}
