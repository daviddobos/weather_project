-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_invalid_cloud_coverage_cd__dbt_tmp_vw" as WITH invalid_cloud_coverage_cd AS (
    SELECT *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
    WHERE 
        ( cloud_coverage_pct BETWEEN 0 AND 20 AND cloud_coverage_cd NOT IN ('Clear'))
        OR 
        ( cloud_coverage_pct BETWEEN 21 AND 40 AND cloud_coverage_cd NOT IN ('Partly Cloudy'))
        OR 
        ( cloud_coverage_pct BETWEEN 41 AND 60 AND cloud_coverage_cd NOT IN ('Cloudy'))
        OR 
        ( cloud_coverage_pct BETWEEN 61 AND 80 AND cloud_coverage_cd NOT IN ('Overcast'))
        OR 
        ( cloud_coverage_pct BETWEEN 81 AND 100 AND cloud_coverage_cd NOT IN ('Completely Overcast'))
)

SELECT *
FROM invalid_cloud_coverage_cd;