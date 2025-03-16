WITH s_weather AS (
    SELECT
        *
    FROM {{ ref('s_weather') }}
)

SELECT * FROM s_weather