{{ config(materialized='table') }}

with calendar as (
  select explode(sequence(
    to_date('{{ var("date_start","2000-01-01") }}'),
    to_date('{{ var("date_end","2035-12-31") }}'),
    interval 1 day
  )) as date_day
)
select
  cast(date_format(date_day,'yyyyMMdd') as int)   as date_sk,
  date_day                                as date,
  year(date_day)                          as year,
  quarter(date_day)                       as quarter,
  concat('Q', quarter(date_day))          as quarter_label,
  month(date_day)                         as month,
  date_format(date_day,'MMMM')            as month_name,
  cast(date_format(date_day,'yyyyMM') as int) as month_key,
  weekofyear(date_day)                    as week_of_year,
  day(date_day)                           as day_of_month,
  dayofweek(date_day)                     as day_of_week_num,
  date_format(date_day,'E')               as day_of_week_name,
  case when dayofweek(date_day) in (1,7) then true else false end as is_weekend,
  last_day(date_day) = date_day           as is_month_end,
  case when last_day(add_months(date_trunc('quarter', date_day), 2)) = date_day
       then true else false end           as is_quarter_end
from calendar
