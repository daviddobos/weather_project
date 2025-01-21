WITH ld_astro_forecast AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'weather_forecast_astro') }}
),

b_astro_forecast AS (
    SELECT
        CONVERT(BIT, is_moon_up) AS moon_up_flg
        ,CONVERT(BIT, is_sun_up) AS sun_up_flg
        ,CONVERT(INT, moon_illumination) AS moon_illumination_no
        ,CONVERT(VARCHAR(20), moon_phase) AS moon_phase_cd
        ,TRY_CONVERT(TIME(0), moonrise) AS moonrise_t
        ,TRY_CONVERT(TIME(0), moonset) AS moonset_t
        ,TRY_CONVERT(TIME(0), sunrise) AS sunrise_t
        ,TRY_CONVERT(TIME(0), sunset) AS sunset_t
        ,CONVERT(VARCHAR(50), city) AS city_nm
        ,CONVERT(VARCHAR(50), country) AS country_nm
        ,CONVERT(DATETIME2(6), forecast_date) AS forecast_dt
        ,CONVERT(DATETIME2(6), p_load_dt) AS p_load_dt
    FROM ld_astro_forecast
)

SELECT * FROM b_astro_forecast