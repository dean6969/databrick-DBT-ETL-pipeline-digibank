{{ config(materialized='table') }}

select
  d.date_pk                             as enrollment_date_pk,
  p.enrollment_pk                       as product_enrollment_pk,
  i.product_id,
  i.customer_id,
  cast(i.credit_limit as decimal(18,2)) as credit_limit
from {{ ref('stg_product_enrollments') }} i
left join {{ ref('dim_date') }} d
  on d.date = cast(i.enrollment_date as date)
-- customer SCD2 as-of tại thời điểm enroll
left join {{ ref('dim_customer') }} c
  on c.customer_id = i.customer_id
 and cast(i.enrollment_date as timestamp) >= c.valid_from
 and (c.valid_to is null or cast(i.enrollment_date as timestamp) < c.valid_to)
-- product-enrollment SCD2 as-of tại thời điểm enroll
left join {{ ref('dim_product') }} p
  on p.product_id = i.product_id
 and cast(i.enrollment_date as timestamp) >= p.valid_from
 and (p.valid_to is null or cast(i.enrollment_date as timestamp) < p.valid_to)
;
