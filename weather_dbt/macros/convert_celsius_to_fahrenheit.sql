{% macro unit_converter(celsius) %}
    (({{ celsius }} * 9 / 5) + 32)
{% endmacro %}