{% test valid_latitude(model, column_name) %}
  
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < -90 or {{ column_name }} > 90)

{% endtest %}
