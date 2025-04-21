WITH s_fct_astro AS (
    SELECT DISTINCT
        county_nm
    FROM {{ ref('s_fct_astro') }}
),

s_fct_astro_forecast AS (
    SELECT DISTINCT
        county_nm
    FROM {{ ref('s_fct_astro_forecast') }}
),

s_fct_weather AS (
    SELECT DISTINCT
        county_nm
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_forecast AS (
    SELECT DISTINCT
        county_nm
    FROM {{ ref('s_fct_weather_forecast') }}
),

union_select AS (
    SELECT * FROM s_fct_astro
    UNION
    SELECT * FROM s_fct_astro_forecast
    UNION
    SELECT * FROM s_fct_weather
    UNION
    SELECT * FROM s_fct_weather_forecast
)

SELECT * FROM union_select