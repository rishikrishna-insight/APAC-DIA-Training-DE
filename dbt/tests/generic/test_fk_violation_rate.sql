{% test fk_violation_rate(model, column_name, to, field, max_violation_rate=1.5) %}

    {# Test to ensure foreign key violation rate is below threshold #}
    
    with total_records as (
        select count(*) as total_count
        from {{ model }}
        where {{ column_name }} is not null
    ),
    
    violations as (
        select count(*) as violation_count
        from {{ model }} m
        where {{ column_name }} is not null
          and not exists (
              select 1 
              from {{ to }} t 
              where t.{{ field }} = m.{{ column_name }}
          )
    ),
    
    violation_rate as (
        select 
            violation_count,
            total_count,
            case 
                when total_count > 0 
                then (violation_count * 100.0 / total_count) 
                else 0 
            end as violation_percentage
        from violations
        cross join total_records
    )
    
    select *
    from violation_rate
    where violation_percentage > {{ max_violation_rate }}

{% endtest %}