WITH invalid_cloud_coverage_cd AS (
    SELECT *
    FROM {{ ref('s_fct_weather_forecast') }}
    WHERE 
        ( cloud_coverage_pct BETWEEN 0 AND 20 AND cloud_coverage_cd NOT IN ('Very Low'))
        OR 
        ( cloud_coverage_pct BETWEEN 21 AND 40 AND cloud_coverage_cd NOT IN ('Low'))
        OR 
        ( cloud_coverage_pct BETWEEN 41 AND 60 AND cloud_coverage_cd NOT IN ('Moderate'))
        OR 
        ( cloud_coverage_pct BETWEEN 61 AND 80 AND cloud_coverage_cd NOT IN ('High'))
        OR 
        ( cloud_coverage_pct BETWEEN 81 AND 100 AND cloud_coverage_cd NOT IN ('Very High'))
)

SELECT *
FROM invalid_cloud_coverage_cd

