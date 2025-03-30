-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_forecast_invalid_temp_feelslike_cd__dbt_tmp_vw" as WITH invalid_temp_feelslike_cd AS (
    SELECT *
    FROM "wh_weather"."dbt_silver"."s_fct_weather_forecast"
    WHERE 
        ( temp_feelslike_no <= 0 AND temp_feelslike_cd NOT IN ('Very Cold'))
        OR 
        ( temp_feelslike_no BETWEEN 1 AND 10 AND temp_feelslike_cd NOT IN ('Cold'))
        OR 
        ( temp_feelslike_no BETWEEN 11 AND 20 AND temp_feelslike_cd NOT IN ('Cool'))
        OR 
        ( temp_feelslike_no BETWEEN 21 AND 30 AND temp_feelslike_cd NOT IN ('Warm'))
        OR 
        ( temp_feelslike_no BETWEEN 31 AND 40 AND temp_feelslike_cd NOT IN ('Hot'))
        OR 
        ( temp_feelslike_no > 41 AND temp_feelslike_cd NOT IN ('Very Hot'))
)

SELECT *
FROM invalid_temp_feelslike_cd;