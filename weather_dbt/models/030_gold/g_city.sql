WITH s_city AS (
    SELECT
        *
    FROM {{ ref('s_city') }}
)

SELECT * FROM s_city