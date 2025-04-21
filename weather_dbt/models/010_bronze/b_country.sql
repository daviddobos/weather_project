WITH country_raw AS (
    SELECT 
        *
    FROM {{ source('seed', 'country') }}
),

b_country AS (
    SELECT
        CONVERT(VARCHAR(2), country_code) AS country_cd
        ,CONVERT(VARCHAR(50), country) AS country_nm
        ,CONVERT(VARCHAR(50), continent) AS continent_nm
        ,CONVERT(BIT, eu_member_flag) AS eu_member_flg
        ,CONVERT(VARCHAR(3), currency) AS currency_cd
        ,CONVERT(VARCHAR(3), iso_alpha_3) AS iso_alpha_3_cd
    FROM country_raw
)

SELECT * FROM b_country