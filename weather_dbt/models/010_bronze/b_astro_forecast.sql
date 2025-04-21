WITH ld_astro_forecast AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'ld_weather_astro_forecast') }}
),

b_astro_forecast AS (
    SELECT
        CONVERT(DATE, m_valid_dt) AS m_valid_dt
        ,CONVERT(BIT, is_moon_up) AS moon_up_flg
        ,CONVERT(BIT, is_sun_up) AS sun_up_flg
        ,CONVERT(INT, moon_illumination) AS moon_illumination_pct
        ,CONVERT(VARCHAR(20), moon_phase) AS moon_phase_cd
        ,CONVERT(VARCHAR(11), moonrise) AS moonrise_t
        ,CONVERT(VARCHAR(10), moonset) AS moonset_t
        ,CONVERT(VARCHAR(10), sunrise) AS sunrise_t
        ,CONVERT(VARCHAR(10), sunset) AS sunset_t
        ,CONVERT(VARCHAR(50), city) AS city_nm
        ,CONVERT(VARCHAR(50), country_EN) AS country_nm
        ,CONVERT(DATE, forecast_date) AS forecast_dt
        ,CONVERT(DATETIME2(6), m_extracted_at_dttm) AS m_extracted_at_dttm
        ,CONVERT(DATETIME2(6), m_updated_at_dttm) AS m_updated_at_dttm
    FROM ld_astro_forecast
)

SELECT * FROM b_astro_forecast