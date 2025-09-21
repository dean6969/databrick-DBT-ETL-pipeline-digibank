{{ config(
    materialized = 'table'
) }}

SELECT
    i.interaction_pk,
    i.interaction_id,
    d.date_pk AS interaction_date_pk,
    i.customer_id,
    i.interaction_type,
    1 AS interaction_count
FROM
    {{ ref('stg_crm_interactions') }}
    i
    LEFT JOIN {{ ref('dim_date') }}
    d
    ON d.date = CAST(
        i.interaction_date AS DATE
    )
    LEFT JOIN {{ ref('dim_customer') }} C
    ON C.customer_id = i.customer_id
    AND CAST(
        i.interaction_date AS TIMESTAMP
    ) >= C.valid_from
    AND (
        C.valid_to IS NULL
        OR CAST(
            i.interaction_date AS TIMESTAMP
        ) < C.valid_to
    )
    LEFT JOIN {{ ref('dim_interactions') }}
    di
    ON di.interaction_pk = i.interaction_pk;
