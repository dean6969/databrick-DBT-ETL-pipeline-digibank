{{ config(
    materialized='table'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    mobile,
    gender,
    date_of_birth,
    signup_date,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snp_customer') }}

{% if is_incremental() %}
-- chỉ insert các version mới từ snapshot
where dbt_valid_from > (select max(valid_from) from {{ this }})
{% endif %}
