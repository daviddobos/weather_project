-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_invalid_snow_chance_cd__dbt_tmp_vw" as WITH invalid_snow_chance_cd AS (
    SELECT *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
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
FROM invalid_snow_chance_cd;