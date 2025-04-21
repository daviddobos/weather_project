{% macro add_unit_conversions_to_km(column_name) %}
    {% set mile_col = column_name | replace('_kph_', '_miph_') %}
    

    {{ column_name }}
    , {{ column_name }} * {{ var('v_km_to_mi') }} AS {{ mile_col }}
{% endmacro %}
