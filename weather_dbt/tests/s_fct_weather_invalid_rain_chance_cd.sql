WITH invalid_rain_chance_cd AS (
    SELECT *
    FROM {{ ref('s_fct_weather') }}
    WHERE 
        ( rain_chance_pct BETWEEN 0 AND 20 AND rain_chance_cd NOT IN ('Very Low'))
        OR 
        ( rain_chance_pct BETWEEN 21 AND 40 AND rain_chance_cd NOT IN ('Low'))
        OR 
        ( rain_chance_pct BETWEEN 41 AND 60 AND rain_chance_cd NOT IN ('Moderate'))
        OR 
        ( rain_chance_pct BETWEEN 61 AND 80 AND rain_chance_cd NOT IN ('High'))
        OR 
        ( rain_chance_pct BETWEEN 81 AND 100 AND rain_chance_cd NOT IN ('Very High'))
)

SELECT *
FROM invalid_rain_chance_cd

