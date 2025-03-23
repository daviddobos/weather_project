{% test unique(model, column_name) %}
    with validation as (
        -- Find all values of the column that appear more than once
        select {{ column_name }}
        from {{ model }}
        group by {{ column_name }}
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from {{ model }}
    where {{ column_name }} in (select {{ column_name }} from validation)

{% endtest %}