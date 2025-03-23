WITH s_fct_weather AS (
    SELECT
        *
    FROM {{ ref('s_fct_weather') }}
)

SELECT * FROM s_fct_weather