WITH s_fct_astro_forecast AS (
    SELECT
        *
    FROM {{ ref('s_fct_astro_forecast') }}
)

SELECT * FROM s_fct_astro_forecast