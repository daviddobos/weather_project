WITH b_astro AS (
    SELECT 
        *
    FROM {{ ref('b_astro') }}
),

s_dim_country AS (
    SELECT
        *
    FROM {{ ref('s_dim_country') }}
),

s_dim_city AS (
    SELECT
        *
    FROM {{ ref('s_dim_city') }}
),

join_country_cd AS (
    SELECT
        b_astro.*
        ,s_dim_country.country_cd AS country_cd
    FROM b_astro
    JOIN s_dim_country
    ON b_astro.country_nm = s_dim_country.country_nm
),


gen_sk AS (
    SELECT
        *
        ,{{ gen_sk(['m_valid_dt','forecast_dt','country_cd', 'city_nm']) }} AS astro_pk
        ,{{ gen_sk(['country_cd', 'city_nm']) }} AS city_fk
    FROM join_country_cd
),

join_county_nm AS (
    SELECT
        gen_sk.*
        ,s_dim_city.county_nm
    FROM gen_sk
    JOIN s_dim_city
    ON gen_sk.city_fk = s_dim_city.city_pk
),

missing_astro_times AS (
    SELECT
        m_valid_dt
        ,astro_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,moon_illumination_pct
        ,moon_phase_cd
        ,CASE 
            WHEN moonrise_t = 'No moonrise' THEN NULL 
            ELSE moonrise_t
        END AS moonrise_t
        ,CASE 
            WHEN moonset_t = 'No moonset' THEN NULL 
            ELSE moonset_t
        END AS moonset_t
        ,CASE 
            WHEN sunrise_t = 'No sunrise' THEN NULL 
            ELSE sunrise_t
        END AS sunrise_t
        ,CASE 
            WHEN sunset_t = 'No sunset' THEN NULL 
            ELSE sunset_t
        END AS sunset_t
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM join_county_nm
),

reorder AS (
    SELECT
        m_valid_dt
        ,astro_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,moon_illumination_pct
        ,moon_phase_cd
        ,CONVERT(TIME(0), moonrise_t) AS moonrise_t
        ,CONVERT(TIME(0), moonset_t) AS moonset_t
        ,CONVERT(TIME(0), sunrise_t) AS sunrise_t
        ,CONVERT(TIME(0), sunset_t) AS sunset_t
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM missing_astro_times
)

SELECT * FROM reorder

