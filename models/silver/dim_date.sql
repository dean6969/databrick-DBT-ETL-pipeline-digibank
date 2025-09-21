{{ config(
    materialized='table'
) }}

with dates as (

    -- tạo danh sách ngày từ 2010 đến 2030
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2099-12-31' as date)"
    ) }}

)

select
    cast(date_day as date) as date,
    cast(date_format(date_day, 'yyyyMMdd') as bigint) as date_pk,
    year(date_day)   as year,
    quarter(date_day) as quarter,
    month(date_day)  as month,
    day(date_day)    as day,
    weekofyear(date_day) as week_of_year,
    dayofweek(date_day)  as day_of_week,   -- 1=Sunday … 7=Saturday (Databricks convention)
    case when dayofweek(date_day) in (1,7) then true else false end as is_weekend,
    last_day(date_day) as month_end_date,
    trunc(date_day, 'MM') as month_start_date
from dates
