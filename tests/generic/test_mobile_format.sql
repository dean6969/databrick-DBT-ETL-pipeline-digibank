{% test mobile_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} like '+63%'
   or {{ column_name }} not like '0%'

{% endtest %}
