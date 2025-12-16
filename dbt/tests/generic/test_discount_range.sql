{% test discount_range(model, column_name, min_value=0, max_value=100) %}

    -- Test to ensure discount percentages are within valid range (0-100%)
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})

{% endtest %}
