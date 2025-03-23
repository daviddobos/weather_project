WITH invalid_temp_feelslike_cd AS (
    SELECT *
    FROM {{ ref('s_fct_weather_forecast') }}
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
        ( temp_feelslike_no > 41 temp_feelslike_cd AND temp_feels_like_cd NOT IN ('Very Hot'))
)

SELECT *
FROM invalid_temp_feelslike_cd

