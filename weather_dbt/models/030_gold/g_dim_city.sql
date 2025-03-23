WITH s_dim_city AS (
    SELECT
        *
    FROM {{ ref('s_dim_city') }}
)

SELECT * FROM s_dim_city