WITH s_astro AS (
    SELECT
        *
    FROM {{ ref('s_astro') }}
)

SELECT * FROM s_astro