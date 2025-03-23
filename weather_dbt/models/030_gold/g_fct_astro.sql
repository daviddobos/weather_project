WITH s_fct_astro AS (
    SELECT
        *
    FROM {{ ref('s_fct_astro') }}
)

SELECT * FROM s_fct_astro