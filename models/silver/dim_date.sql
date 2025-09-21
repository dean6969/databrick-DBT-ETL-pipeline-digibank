{{ config(
    materialized = 'table'
) }}

WITH dates AS (
    -- CREATE automatically DATE {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2010-01-01' as date)",
        end_date = "cast('2099-12-31' as date)"
    ) }}
)
SELECT
    CAST(
        date_day AS DATE
    ) AS DATE,
    CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_pk,
    YEAR(date_day) AS YEAR,
    quarter(date_day) AS quarter,
    MONTH(date_day) AS MONTH,
    DAY(date_day) AS DAY,
    weekofyear(date_day) AS week_of_year,
    dayofweek(date_day) AS day_of_week,- - 1 = sunday … 7 = saturday (
        databricks convention
    ) CASE
        WHEN dayofweek(date_day) IN (
            1,
            7
        ) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    LAST_DAY(date_day) AS month_end_date,
    TRUNC(
        date_day,
        'MM'
    ) AS month_start_date
FROM
    dates
