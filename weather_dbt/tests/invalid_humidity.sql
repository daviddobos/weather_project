{% test invalid_humidity(model, column_name) %}

WITH invalid_humidity AS (
    SELECT *
    FROM {{ model }}
    WHERE 
        ( {{ column_name }} BETWEEN 0 AND 20 AND humidity_cd NOT IN ('Very Low'))
        OR 
        ( {{ column_name }} BETWEEN 21 AND 40 AND humidity_cd NOT IN ('Low'))
        OR 
        ( {{ column_name }} BETWEEN 41 AND 60 AND humidity_cd NOT IN ('Moderate'))
        OR 
        ( {{ column_name }} BETWEEN 61 AND 80 AND humidity_cd NOT IN ('High'))
        OR 
        ( {{ column_name }} BETWEEN 81 AND 100 AND humidity_cd NOT IN ('Very High'))
)

SELECT *
FROM invalid_humidity

{% endtest %}
