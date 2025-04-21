WITH distinct_m_valid_dt AS (
    SELECT DISTINCT
        m_valid_dt
    FROM {{ ref('s_fct_weather') }}
)

SELECT * FROM distinct_m_valid_dt