WITH seed_city AS (
    SELECT
        CONVERT(VARCHAR(50), city_nm) AS city_nm
        ,CONVERT(INT, ksh_cd) AS ksh_cd
        ,CONVERT(VARCHAR(50), city_type_cd) AS city_type_cd
        ,CONVERT(VARCHAR(50), county_nm) AS county_nm
        ,CONVERT(VARCHAR(50), district_cd) AS district_cd
        ,CONVERT(VARCHAR(50), district_nm) AS district_nm
        ,CONVERT(VARCHAR(50), district_seat_nm) AS district_seat_nm
        ,CONVERT(DECIMAL, area_no) area_no
        ,CONVERT(INT, population_no) AS population_no
        ,CONVERT(INT, apartments_no) AS apartments_no
        ,CONVERT(VARCHAR(2), country_cd) AS country_cd
        ,CONVERT(FLOAT, latitude_no) AS latitude_no
        ,CONVERT(FLOAT, longitude_no) AS longitude_no
    FROM {{ source('seed', 'city') }}  
)

SELECT * FROM seed_city
