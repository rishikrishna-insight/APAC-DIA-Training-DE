{% test email_format(model, column_name) %}
  
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and not regexp_full_match(
      lower({{ column_name }}), 
      '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'
    )

{% endtest %}