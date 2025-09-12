{% macro convert_adelaide_to_utc(column_name) %}
    {# Convert Adelaide timestamp to UTC, returning simple TIMESTAMP type #}
    cast(
        cast({{ column_name }} as timestamp) at time zone 'Australia/Adelaide' at time zone 'UTC' 
        as timestamp
    )
{% endmacro %}

{% macro current_utc_timestamp() %}
    {# Get current timestamp in UTC, returning simple TIMESTAMP type #}
    cast(
        current_timestamp at time zone 'UTC' 
        as timestamp
    )
{% endmacro %}
