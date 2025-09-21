{{ config(
  materialized = 'table'
) }}

SELECT
  interaction_pk,
  interaction_id,
  interaction_type
FROM
  {{ ref('stg_crm_interactions') }}
WHERE
  interaction_type IS NOT NULL
  AND TRIM(interaction_type) <> ''
