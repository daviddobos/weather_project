{% macro add_unit_conversions_to_kph(column_name) %}
    {% set mile_col = column_name | replace('_kph_', '_miph_') %}
    
    {{ column_name }} * {{ var('v_km_to_mi') }} AS {{ mile_col }}
{% endmacro %}
