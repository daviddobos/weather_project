WITH ld_astro AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'ld_weather_astro') }}
),

b_astro AS (
    SELECT
        CONVERT(DATE, m_valid_dt) AS m_valid_dt
        ,CONVERT(INT, moon_illumination) AS moon_illumination_no
        ,CONVERT(VARCHAR(20), moon_phase) AS moon_phase_cd
        ,CONVERT(TIME(0), moonrise) AS moonrise_t
        ,CONVERT(TIME(0), moonset) AS moonset_t
        ,CONVERT(TIME(0), sunrise) AS sunrise_t
        ,CONVERT(TIME(0), sunset) AS sunset_t
        ,CONVERT(VARCHAR(50), city) AS city_nm
        ,CONVERT(VARCHAR(50), country_EN) AS country_nm
        ,CONVERT(DATETIME2(6), forecast_date) AS forecast_dt
        ,CONVERT(DATETIME2(6), m_extracted_at_dttm) AS m_extracted_at_dttm
        ,CONVERT(DATETIME2(6), m_updated_at_dttm) AS m_updated_at_dttm
    FROM ld_astro
)

SELECT * FROM b_astro