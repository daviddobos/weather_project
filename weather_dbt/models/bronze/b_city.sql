WITH ld_cities AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'ld_city') }}
),

b_city AS (
    SELECT
        CONVERT(VARCHAR(2), country) AS country_cd
        ,CONVERT(INT, id) as id
        ,CONVERT(VARCHAR(400))
        ,CONVERT(DECIMAL())
)

SELECT * FROM b_city
{# 
    ,CONVERT(DECIMAL({{var('decimal_precision')}}, {{var('decimal_scale')}}), duration_in_ms) AS duration_in_ms_num
    ,CONVERT(VARCHAR(8000), [message]) AS message_txt
    ,CONVERT(VARCHAR(4), m_source_system_cd) AS m_source_system_cd 
    ,CONVERT(DATETIME2(6), m_extracted_dttm) AS m_extracted_dttm  
    ,CONVERT(DATETIME2(6), GETUTCDATE()) AS m_updated_at_dttm  #}
