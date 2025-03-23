{% test not_null (model, column_name) %}

with validation as (
    SELECT
        *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
)

select * from validation

{% endtest %}