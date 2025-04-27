{% macro add_unit_conversions_to_cels(column_name) %}
    {% set fahrenheit_col = column_name | replace('_c_', '_f_') %}
    {% set kelvin_col = column_name | replace('_c_', '_k_') %}

    {{ column_name }} * 9/5 + 32 AS {{ fahrenheit_col }}
    , {{ column_name }} + 273.15 AS {{ kelvin_col }}
{% endmacro %}
