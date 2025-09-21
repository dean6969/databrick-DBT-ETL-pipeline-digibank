{{ config(
    materialized='table'
) }}

with base as (

    -- Sinh số giây trong ngày (0 → 86399)
    select explode(sequence(0, 86399, 1)) as second_of_day

),

final as (

    select
        second_of_day,
        floor(second_of_day / 3600) as hour,
        floor((second_of_day % 3600) / 60) as minute,
        second_of_day % 60 as second,

        lpad(cast(floor(second_of_day / 3600) as string), 2, '0')
          || ':' ||
        lpad(cast(floor((second_of_day % 3600) / 60) as string), 2, '0')
          || ':' ||
        lpad(cast(second_of_day % 60 as string), 2, '0') as time_label

    from base
)

select
    md5(cast(second_of_day as string)) as time_pk,
    second_of_day,
    hour,
    minute,
    second,
    time_label
from final
