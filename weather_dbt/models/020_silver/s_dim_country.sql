WITH b_country AS (
    SELECT 
        *
    FROM {{ ref('b_country') }}
),

reorder AS (
    SELECT 
        country_cd
        ,country_nm
        ,continent_nm
        ,eu_member_flg
        ,currency_cd
        ,iso_alpha_3_cd
    FROM b_country
)

SELECT * FROM reorder