WITH ld_city AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'city') }}
),

b_city AS (
    SELECT
        CONVERT(VARCHAR(50), Country) AS country_cd
        ,CONVERT(VARCHAR(50), County) AS county_nm
        ,CONVERT(VARCHAR(50), City) AS city_nm
    FROM ld_city
)

SELECT * FROM b_city


{# 
    ,CONVERT(DECIMAL({{var('decimal_precision')}}, {{var('decimal_scale')}}), duration_in_ms) AS duration_in_ms_num
    ,CONVERT(VARCHAR(8000), [message]) AS message_txt
    ,CONVERT(VARCHAR(4), m_source_system_cd) AS m_source_system_cd 
    ,CONVERT(DATETIME2(6), m_extracted_dttm) AS m_extracted_dttm  
    ,CONVERT(DATETIME2(6), GETUTCDATE()) AS m_updated_at_dttm  #}
