WITH b_weather AS (
    SELECT 
        *
    FROM {{ref('b_weather') }}
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
        b_weather.*
        ,s_dim_country.country_cd AS country_cd
    FROM b_weather
    JOIN s_dim_country
    ON b_weather.country_nm = s_dim_country.country_nm
),

gen_sk AS (
    SELECT
        *
        ,{{ gen_sk(['m_valid_dt', 'forecast_dt', 'country_cd', 'city_nm', 'time_epoch']) }} AS weather_pk
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

unit_conversion AS (
    SELECT
        *
        ,{{ add_unit_conversions_to_kph('gust_kph_no') }}
        ,{{ add_unit_conversions_to_cels('temp_c_no') }}
        ,{{ add_unit_conversions_to_cels('heatindex_c_no') }}
        ,vis_km_no * {{ var('v_km_to_mi') }} AS vis_mi_no
        ,{{ add_unit_conversions_to_kph('wind_kph_no') }}
        ,{{ add_unit_conversions_to_cels('windchill_c_no') }}
    FROM join_county_nm
),

weather_categories AS (
    SELECT
        *
        -- Rain Chance Category
        ,CASE
            WHEN rain_chance_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN rain_chance_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN rain_chance_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN rain_chance_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN rain_chance_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS rain_chance_cd

        -- Snow Chance Category
        ,CASE
            WHEN snow_chance_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN snow_chance_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN snow_chance_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN snow_chance_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN snow_chance_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS snow_chance_cd

        -- Cloud Coverage Category
        ,CASE
            WHEN cloud_coverage_pct BETWEEN 0 AND 20 THEN 'Clear'
            WHEN cloud_coverage_pct BETWEEN 21 AND 40 THEN 'Partly Cloudy'
            WHEN cloud_coverage_pct BETWEEN 41 AND 60 THEN 'Cloudy'
            WHEN cloud_coverage_pct BETWEEN 61 AND 80 THEN 'Overcast'
            WHEN cloud_coverage_pct BETWEEN 81 AND 100 THEN 'Completely Overcast'
            ELSE 'Unknown'
        END AS cloud_coverage_cd
        -- Temperature Category - Cels
        ,CASE
            WHEN temp_c_no <= 0 THEN 'Very Cold'
            WHEN temp_c_no BETWEEN 1 AND 10 THEN 'Cold'
            WHEN temp_c_no BETWEEN 11 AND 20 THEN 'Cool'
            WHEN temp_c_no BETWEEN 21 AND 30 THEN 'Warm'
            WHEN temp_c_no BETWEEN 31 AND 40 THEN 'Hot'
            WHEN temp_c_no > 40 THEN 'Very Hot'
            ELSE 'Unknown'
        END AS temp_c_cd
        -- Temperature Category - Kelvin
        ,CASE
            WHEN temp_k_no <= 273.15 THEN 'Very Cold'              -- 0°C
            WHEN temp_k_no BETWEEN 274 AND 283 THEN 'Cold'          -- 1-10°C
            WHEN temp_k_no BETWEEN 284 AND 293 THEN 'Cool'          -- 11-20°C
            WHEN temp_k_no BETWEEN 294 AND 303 THEN 'Warm'          -- 21-30°C
            WHEN temp_k_no BETWEEN 304 AND 313 THEN 'Hot'           -- 31-40°C
            WHEN temp_k_no > 313 THEN 'Very Hot'                    -- >40°C
            ELSE 'Unknown'
        END AS temp_k_cd
        -- Temperature Category - Fahr
        ,CASE
            WHEN temp_f_no <= 32 THEN 'Very Cold'                  -- 0°C = 32°F
            WHEN temp_f_no BETWEEN 33 AND 50 THEN 'Cold'            -- 1-10°C ≈ 33-50°F
            WHEN temp_f_no BETWEEN 51 AND 68 THEN 'Cool'            -- 11-20°C ≈ 51-68°F
            WHEN temp_f_no BETWEEN 69 AND 86 THEN 'Warm'            -- 21-30°C ≈ 69-86°F
            WHEN temp_f_no BETWEEN 87 AND 104 THEN 'Hot'            -- 31-40°C ≈ 87-104°F
            WHEN temp_f_no > 104 THEN 'Very Hot'                    -- >40°C ≈ >104°F
            ELSE 'Unknown'
        END AS temp_f_cd
        -- Humidity Category
        ,CASE
            WHEN humidity_pct BETWEEN 0 AND 20 THEN 'Very Low'
            WHEN humidity_pct BETWEEN 21 AND 40 THEN 'Low'
            WHEN humidity_pct BETWEEN 41 AND 60 THEN 'Moderate'
            WHEN humidity_pct BETWEEN 61 AND 80 THEN 'High'
            WHEN humidity_pct BETWEEN 81 AND 100 THEN 'Very High'
            ELSE 'Unknown'
        END AS humidity_cd
        -- Wind Category - kph
        ,CASE
            WHEN wind_kph_no BETWEEN 0 AND 10 THEN 'Calm'
            WHEN wind_kph_no BETWEEN 11 AND 20 THEN 'Light Breeze'
            WHEN wind_kph_no BETWEEN 21 AND 40 THEN 'Moderate Breeze'
            WHEN wind_kph_no BETWEEN 41 AND 60 THEN 'Strong Breeze'
            WHEN wind_kph_no BETWEEN 61 AND 80 THEN 'Gale'
            WHEN wind_kph_no BETWEEN 81 AND 100 THEN 'Strong Gale'
            WHEN wind_kph_no > 100 THEN 'Storm'
            ELSE 'Unknown'
        END AS wind_kph_cd
         -- Wind Category - miph
        ,CASE
            WHEN wind_miph_no BETWEEN 0 AND 6 THEN 'Calm'
            WHEN wind_miph_no BETWEEN 7 AND 12 THEN 'Light Breeze'
            WHEN wind_miph_no BETWEEN 13 AND 25 THEN 'Moderate Breeze'
            WHEN wind_miph_no BETWEEN 26 AND 37 THEN 'Strong Breeze'
            WHEN wind_miph_no BETWEEN 38 AND 50 THEN 'Gale'
            WHEN wind_miph_no BETWEEN 51 AND 62 THEN 'Strong Gale'
            WHEN wind_miph_no > 62 THEN 'Storm'
            ELSE 'Unknown'
        END AS wind_miph_cd
        -- Precip Category - mm
        ,CASE
            WHEN precip_mm_no = 0 THEN 'No Rain'
            WHEN precip_mm_no BETWEEN 0.1 AND 2 THEN 'Light Rain'
            WHEN precip_mm_no BETWEEN 2.1 AND 10 THEN 'Moderate Rain'
            WHEN precip_mm_no BETWEEN 10.1 AND 30 THEN 'Heavy Rain'
            WHEN precip_mm_no BETWEEN 30.1 AND 60 THEN 'Very Heavy Rain'
            WHEN precip_mm_no > 60 THEN 'Extreme Rain'
            ELSE 'Unknown'
        END AS precip_mm_cd
    FROM unit_conversion
),

reorder AS (
    SELECT
        m_valid_dt
        ,weather_pk
        ,forecast_dt
        ,city_nm
        ,city_fk
        ,country_cd
        ,country_nm
        ,county_nm
        ,cloud_coverage_pct
        ,cloud_coverage_cd
        ,gust_kph_no
        ,gust_miph_no
        ,heatindex_c_no
        ,heatindex_f_no
        ,heatindex_k_no
        ,humidity_pct
        ,humidity_cd
        ,is_day_flg
        ,precip_mm_no
        ,precip_mm_cd
        ,pressure_mb_no
        ,temp_c_no
        ,temp_c_cd
        ,temp_f_no
        ,temp_f_cd
        ,temp_k_no
        ,temp_k_cd
        ,temp_feelslike_no
        ,date_dtt
        ,time_epoch
        ,rain_chance_pct
        ,rain_chance_cd
        ,snow_chance_pct
        ,snow_chance_cd
        ,uv_no
        ,vis_km_no
        ,vis_mi_no
        ,wind_dir_cd
        ,wind_kph_no
        ,wind_kph_cd
        ,wind_miph_no
        ,wind_miph_cd
        ,windchill_c_no
        ,windchill_f_no
        ,windchill_k_no
        ,m_extracted_at_dttm
        ,m_updated_at_dttm
    FROM weather_categories
)

SELECT * FROM reorder
