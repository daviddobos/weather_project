WITH s_fct_weather_forecast AS (
    SELECT
        *
    FROM {{ ref('s_fct_weather_forecast') }}
)

SELECT * FROM s_fct_weather_forecast