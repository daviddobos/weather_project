-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_forecast_invalid_rain_chance_cd__dbt_tmp_vw" as WITH invalid_rain_chance_cd AS (
    SELECT *
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
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
FROM invalid_rain_chance_cd;