WITH s_astro_forecast AS (
    SELECT
        *
    FROM {{ ref('s_astro_forecast') }}
)

SELECT * FROM s_astro_forecast