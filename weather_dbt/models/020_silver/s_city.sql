WITH b_city AS (
    SELECT 
        *
    FROM {{ ref('b_city') }}
),

city_detailed AS (
    SELECT
        city_nm
        ,ksh_cd
        ,city_type_cd
        ,county_nm
        ,district_cd
        ,district_nm
        ,district_seat_nm
        ,area_no
        ,population_no
        ,apartments_no
        ,country_cd
    FROM {{ source('seed', 'city') }}  
),

join_detailed AS (
    SELECT
        city_detailed.*
    FROM b_city
    JOIN city_detailed
    ON b_city.city_nm = city_detailed.city_nm
    AND b_city.country_cd = city_detailed.country_cd
    AND b_city.county_nm = city_detailed.county_nm
),

gen_sk AS (
    SELECT
        *
        ,{{ gen_sk(['country_cd', 'city_nm']) }} AS city_pk   --county_cd needs to be added to weather tables then here
    FROM join_detailed
),

reorder AS (
    SELECT
        city_pk
        ,country_cd
        ,county_nm
        ,city_nm
        ,ksh_cd
        ,city_type_cd
        ,district_cd
        ,district_nm
        ,district_seat_nm
        ,area_no
        ,population_no
        ,apartments_no
    FROM gen_sk
)

SELECT * FROM reorder

