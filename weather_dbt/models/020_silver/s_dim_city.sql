WITH b_city AS (
    SELECT 
        *
    FROM {{ ref('b_city') }}
),

gen_sk AS (
    SELECT
        *
        ,{{ gen_sk(['country_cd', 'city_nm']) }} AS city_pk   --county_cd needs to be added to weather tables then here
    FROM b_city
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
        ,latitude_no
        ,longitude_no
    FROM gen_sk
)

SELECT * FROM reorder

