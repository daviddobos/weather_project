WITH invalid_humidity_cd AS (
    SELECT *
    FROM {{ ref('s_fct_weather') }}
    WHERE 
        ( humidity_pct BETWEEN 0 AND 20 AND humidity_cd NOT IN ('Very Low'))
        OR 
        ( humidity_pct BETWEEN 21 AND 40 AND humidity_cd NOT IN ('Low'))
        OR 
        ( humidity_pct BETWEEN 41 AND 60 AND humidity_cd NOT IN ('Moderate'))
        OR 
        ( humidity_pct BETWEEN 61 AND 80 AND humidity_cd NOT IN ('High'))
        OR 
        ( humidity_pct BETWEEN 81 AND 100 AND humidity_cd NOT IN ('Very High'))
)

SELECT *
FROM invalid_humidity_cd

