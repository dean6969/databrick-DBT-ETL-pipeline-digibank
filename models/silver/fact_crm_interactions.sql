{{ config(materialized='table') }}

-- 1) source
with base as (
  select
    cast(interaction_id as int)    as interaction_id,
    cast(customer_id as int)       as customer_id,
    upper(trim(interaction_type))  as interaction_type,
    cast(interaction_date as date) as interaction_date
  from {{ ref('stg_crm_interactions') }}
),

-- 2) SCD2 dim
dim as (
  select
    customer_sk,
    customer_id,
    valid_from,
    coalesce(valid_to, to_timestamp('2999-12-31')) as valid_to,
    is_current
  from {{ ref('dim_customer_scd2') }}
),

-- 3) PIT + fallback; rank = 0 (PIT), 1 (current), 2 (no match)
joined as (
  select
    b.*,
    d.customer_sk,
    case
      when to_timestamp(b.interaction_date) >= d.valid_from
       and to_timestamp(b.interaction_date) <  d.valid_to then 0
      when d.is_current = true then 1
      else 2
    end as join_rank,
    d.valid_from
  from base b
  left join dim d
    on d.customer_id = b.customer_id
),

ranked as (
  select
    interaction_id,
    customer_id,
    customer_sk,
    interaction_type,
    interaction_date,
    row_number() over (
      partition by interaction_id
      order by join_rank asc, valid_from desc
    ) as rn
  from joined
)

-- keep best match, and REQUIRE a mapped customer_sk to pass not_null test
select
  interaction_id,
  customer_id,
  customer_sk,
  interaction_type,
  interaction_date,
  cast(date_format(interaction_date,'yyyyMMdd') as int) as date_sk,
  1 as interaction_cnt
from ranked
where rn = 1
  and customer_sk is not null;
