{{ config(materialized='table') }}

select
    i.interaction_pk,
    i.interaction_id,
  d.date_pk                       as interaction_date_pk,
  i.customer_id,
i.interaction_type,
  1 as interaction_count
from {{ ref('stg_crm_interactions') }} i
left join {{ ref('dim_date') }} d
  on d.date = cast(i.interaction_date as date)
left join {{ ref('dim_customer') }} c
  on c.customer_id = i.customer_id
 and cast(i.interaction_date as timestamp) >= c.valid_from
 and (c.valid_to is null or cast(i.interaction_date as timestamp) < c.valid_to)
left join {{ ref('dim_interactions') }} di
  on di.interaction_pk = i.interaction_pk
;
