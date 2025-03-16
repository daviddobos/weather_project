WITH ld_weather AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'ld_weather_measure') }}
),

b_weather AS (
    SELECT
        CONVERT(DATE, m_valid_dt) AS m_valid_dt
        ,CONVERT(INT, chance_of_rain) AS rain_chance_no
        ,CONVERT(INT, chance_of_snow) AS snow_chance_no
        ,CONVERT(INT, cloud) AS cloud_coverage_no
        ,CONVERT(FLOAT, feelslike_c) AS temp_feelslike_no
        ,CONVERT(FLOAT, gust_kph) AS gust_kph_no
        ,CONVERT(INT, humidity) AS humidity_no
        ,CONVERT(FLOAT, heatindex_c) AS heatindex_c_no
        ,CONVERT(BIT, is_day) AS is_day_flg
        ,CONVERT(FLOAT, precip_mm) AS precip_mm_no
        ,CONVERT(FLOAT, pressure_mb) AS pressure_mb_no
        ,CONVERT(FLOAT, temp_c) AS temp_c_no
        ,CONVERT(DATETIME2(6), time) date_dtt
        ,CONVERT(INT, time_epoch) AS time_epoch
        ,CONVERT(FLOAT, uv) AS uv_no
        ,CONVERT(FLOAT, vis_km) AS vis_km_no
        ,CONVERT(VARCHAR(8), wind_dir) AS wind_dir_cd
        ,CONVERT(FLOAT, wind_kph) AS wind_kph_no
        ,CONVERT(FLOAT, windchill_c) AS windchill_c_no
        ,CONVERT(VARCHAR(30), city) AS city_nm
        ,CONVERT(VARCHAR(30), country_EN) AS country_nm
        ,CONVERT(DATE, forecast_date) AS forecast_dt
        ,CONVERT(DATETIME2(6), m_extracted_at_dttm) AS m_extracted_at_dttm
        ,CONVERT(DATETIME2(6), m_updated_at_dttm) AS m_updated_at_dttm
    FROM ld_weather
)

SELECT * FROM b_weather