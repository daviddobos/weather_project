{% test relationships(model, column_name, to, field) %}
    with validation as (

        -- Find all records in the child model where the foreign key does not exist in the parent model
        select child.*
        from {{ model }} as child
        left join {{ to }} as parent
        on child.{{ column_name }} = parent.{{ field }}
        where parent.{{ field }} is null and child.{{ column_name }} is not null
    )

    -- Return all records from the child model that have unmatched foreign keys
    select *
    from validation

{% endtest %}