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
