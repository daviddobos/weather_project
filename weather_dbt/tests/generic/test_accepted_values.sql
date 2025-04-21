{% test accepted_values(model, column_name, values) %}

with validation as (
    select *
    from {{ model }}
    where {{ column_name }} not in 
        {{ '(' }}

        {% for val in values %}
            '{{ val }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
   {{ ')' }}
)

select * from validation

{% endtest %}
