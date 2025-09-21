{{ config(
    materialized = 'table'
) }}

WITH base AS (

    SELECT
        explode(SEQUENCE(0, 86399, 1)) AS second_of_day
),
FINAL AS (
    SELECT
        second_of_day,
        FLOOR(
            second_of_day / 3600
        ) AS HOUR,
        FLOOR((second_of_day % 3600) / 60) AS MINUTE,
        second_of_day % 60 AS SECOND,
        LPAD(CAST(FLOOR(second_of_day / 3600) AS STRING), 2, '0') || ':' || LPAD(CAST(FLOOR((second_of_day % 3600) / 60) AS STRING), 2, '0') || ':' || LPAD(CAST(second_of_day % 60 AS STRING), 2, '0') AS time_label
    FROM
        base
)
SELECT
    MD5(CAST(second_of_day AS STRING)) AS time_pk,
    second_of_day,
    HOUR,
    MINUTE,
    SECOND,
    time_label
FROM
    FINAL
