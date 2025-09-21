{{ config(
    materialized = 'table'
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    mobile,
    gender,
    date_of_birth,
    signup_date,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
FROM
    {{ ref('snp_customer') }}

{% if is_incremental() %}
WHERE
    dbt_valid_from > (
        SELECT
            MAX(valid_from)
        FROM
            {{ this }}
    )
{% endif %}
