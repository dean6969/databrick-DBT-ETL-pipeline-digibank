{% snapshot snp_customer %}
{{
  config(
    target_database='digibank',
    target_schema='digi_dev_silver',
    unique_key='customer_id',
    strategy='check',
    check_cols=['first_name','last_name','email','mobile','gender','date_of_birth','signup_date']
  )
}}
select
  customer_id,
  first_name,
  last_name,
  email,
  mobile,
  gender,
  date_of_birth,
  signup_date
from {{ ref('stg_customer') }}
{% endsnapshot %}
