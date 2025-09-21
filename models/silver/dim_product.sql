{{ config(
  materialized = 'table'
) }}

SELECT
  enrollment_pk,
  product_id AS product_id,
  product_type,
  dbt_valid_from AS valid_from,
  dbt_valid_to AS valid_to,
  CASE
    WHEN dbt_valid_to IS NULL THEN TRUE
    ELSE FALSE
  END AS is_current
FROM
  {{ ref('snp_product_enrollments') }}
