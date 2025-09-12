{% test valid_longitude(model, column_name) %}
  
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < -180 or {{ column_name }} > 180)

{% endtest %}
