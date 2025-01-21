WITH b_astro_forecast AS (
    SELECT 
        *
    FROM {{ ref('b_astro_forecast') }}
),

country_codes AS (
    SELECT
        *
    FROM {{ source('int', 'country_codes') }}
),

join_country_cd AS (
    SELECT
        b_astro_forecast.*
        ,country_codes.country_cd AS country_cd
    FROM b_astro_forecast
    JOIN country_codes
    ON b_astro_forecast.country_nm = country_codes.country_nm
),

gen_sk AS (
    SELECT
        *
        ,{{ gen_sk(['forecast_dt','country_cd', 'city_nm']) }} AS astro_forecast_pk
    FROM join_country_cd
),

reorder AS (
    SELECT
        astro_forecast_pk
        ,forecast_dt
        ,city_nm
        ,country_cd
        ,country_nm
        ,moon_up_flg
        ,sun_up_flg
        ,moon_illumination_no
        ,moon_phase_cd
        ,moonrise_t
        ,moonset_t
        ,sunrise_t
        ,sunset_t
        ,p_load_dt
    FROM gen_sk
)

SELECT * FROM reorder