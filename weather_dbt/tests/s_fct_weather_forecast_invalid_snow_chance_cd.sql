WITH invalid_snow_chance_cd AS (
    SELECT *
    FROM {{ ref('s_fct_weather_forecast') }}
    WHERE 
        ( snow_chance_pct BETWEEN 0 AND 20 AND snow_chance_cd NOT IN ('Very Low'))
        OR 
        ( snow_chance_pct BETWEEN 21 AND 40 AND snow_chance_cd NOT IN ('Low'))
        OR 
        ( snow_chance_pct BETWEEN 41 AND 60 AND snow_chance_cd NOT IN ('Moderate'))
        OR 
        ( snow_chance_pct BETWEEN 61 AND 80 AND snow_chance_cd NOT IN ('High'))
        OR 
        ( snow_chance_pct BETWEEN 81 AND 100 AND snow_chance_cd NOT IN ('Very High'))
)

SELECT *
FROM invalid_snow_chance_cd

