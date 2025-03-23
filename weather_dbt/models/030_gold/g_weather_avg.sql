WITH base_weather AS (
    SELECT
        weather_pk
        ,city_nm
        ,country_cd
        --,county_cd
        ,forecast_dt
        -- Group to the start of the month
        ,CAST(YEAR(forecast_dt) AS VARCHAR) + '-' + RIGHT('0' + CAST(MONTH(forecast_dt) AS VARCHAR), 2) + '-01' AS month_dt
        ,temp_c_no
        ,temp_feelslike_no
        ,cloud_coverage_no
        ,precip_mm_no
        ,humidity_no
        ,gust_kph_no
        ,heatindex_c_no
        ,wind_kph_no
        ,windchill_c_no
    FROM {{ ref('s_fct_weather') }}
),

base_astro AS (
    SELECT
        astro_pk
        ,forecast_dt
        ,city_nm
        ,country_cd
        ,country_nm
        ,moon_illumination_no
        ,moon_phase_cd
        ,moonrise_t
        ,moonset_t
        ,sunrise_t
        ,sunset_t
    FROM {{ ref('s_fct_astro') }}
),

join_astro AS (
    SELECT
    weather.*
    ,astro.moon_illumination_no
    ,astro.moonrise_t
    ,astro.moonset_t
    ,astro.sunrise_t
    ,astro.sunset_t
    FROM base_weather weather
    JOIN base_astro astro
    ON weather.weather_pk = astro.astro_pk
),

-- Step 1: Pre-aggregate by city, country, and month
monthly_aggregates AS (
    SELECT
        city_nm
        ,country_cd
        ,month_dt
        ,AVG(temp_c_no) AS temp_c_avg
        ,AVG(cloud_coverage_no) AS cloud_coverage_avg
        ,AVG(precip_mm_no) AS precipitation_mm_avg
        ,AVG(humidity_no) AS humidity_avg
        ,AVG(gust_kph_no) AS gust_kph_avg
        ,AVG(heatindex_c_no) AS heatindex_c_no_avg
        ,AVG(wind_kph_no) AS wind_kph_no_avg
        ,AVG(windchill_c_no) AS windchill_c_no_avg
-- Convert TIME to seconds for averaging
        ,AVG(DATEDIFF(SECOND, '00:00:00', moonrise_t)) AS moonrise_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', moonset_t)) AS moonset_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', sunrise_t)) AS sunrise_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', sunset_t)) AS sunset_t_avg
    FROM join_astro
    GROUP BY city_nm, country_cd, month_dt
),

-- Step 2: Calculate rolling averages
rolling_averages AS (
    SELECT
        city_nm
        ,country_cd
        ,month_dt
        -- Rolling averages (2 months)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_2m

        ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_2m
        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_2m
        ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_2m
        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_2m
        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_2m
        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_2m
        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_2m
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_2m
        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_2m
        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_2m
        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_2m

        -- Rolling averages (4 months)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_4m
       ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_4m
        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_4m
        ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_4m
        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_4m
        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_4m
        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_4m
        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_4m
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_4m
        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_4m
        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_4m
        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_4m

        -- Rolling averages (6 months)
        ,AVG(temp_c_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS temperature_c_avg_6m
        ,AVG(cloud_coverage_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS cloud_coverage_avg_6m
        ,AVG(precipitation_mm_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS precipitation_mm_avg_6m
                ,AVG(humidity_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS humidity_avg_6m
        ,AVG(gust_kph_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS gust_kph_avg_6m
        ,AVG(heatindex_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS heatindex_c_no_avg_6m
        ,AVG(wind_kph_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS wind_kph_no_avg_6m
        ,AVG(windchill_c_no_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS windchill_c_no_avg_6m
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_6m
        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_6m
        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_6m
        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, country_cd
            ORDER BY month_dt ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_6m

    FROM monthly_aggregates
),

astro_data_to_time AS (
    SELECT
        city_nm
        ,country_cd
        ,month_dt
        ,temperature_c_avg_2m
        ,temperature_c_avg_4m
        ,temperature_c_avg_6m
        ,cloud_coverage_avg_2m
        ,cloud_coverage_avg_4m
        ,cloud_coverage_avg_6m
        ,precipitation_mm_avg_2m
        ,precipitation_mm_avg_4m
        ,precipitation_mm_avg_6m
        ,humidity_avg_2m
        ,humidity_avg_4m
        ,humidity_avg_6m
        ,gust_kph_avg_2m
        ,gust_kph_avg_4m
        ,gust_kph_avg_6m
        ,heatindex_c_no_avg_2m
        ,heatindex_c_no_avg_4m
        ,heatindex_c_no_avg_6m
        ,wind_kph_no_avg_2m
        ,wind_kph_no_avg_4m
        ,wind_kph_no_avg_6m
        ,windchill_c_no_avg_2m
        ,windchill_c_no_avg_4m
        ,windchill_c_no_avg_6m

        ,CAST(DATEADD(SECOND, moonrise_t_avg_2m, '00:00:00') AS TIME(0)) AS moonrise_t_avg_2m
        ,CAST(DATEADD(SECOND, moonrise_t_avg_4m, '00:00:00') AS TIME(0)) AS moonrise_t_avg_4m
        ,CAST(DATEADD(SECOND, moonrise_t_avg_6m, '00:00:00') AS TIME(0)) AS moonrise_t_avg_6m
        ,CAST(DATEADD(SECOND, moonset_t_avg_2m, '00:00:00') AS TIME(0)) AS moonset_t_avg_2m
        ,CAST(DATEADD(SECOND, moonset_t_avg_4m, '00:00:00') AS TIME(0)) AS moonset_t_avg_4m
        ,CAST(DATEADD(SECOND, moonset_t_avg_6m, '00:00:00') AS TIME(0)) AS moonset_t_avg_6m
        ,CAST(DATEADD(SECOND, sunrise_t_avg_2m, '00:00:00') AS TIME(0)) AS sunrise_t_avg_2m
        ,CAST(DATEADD(SECOND, sunrise_t_avg_4m, '00:00:00') AS TIME(0)) AS sunrise_t_avg_4m
        ,CAST(DATEADD(SECOND, sunrise_t_avg_6m, '00:00:00') AS TIME(0)) AS sunrise_t_avg_6m
        ,CAST(DATEADD(SECOND, sunset_t_avg_2m, '00:00:00') AS TIME(0)) AS sunset_t_avg_2m
        ,CAST(DATEADD(SECOND, sunset_t_avg_4m, '00:00:00') AS TIME(0)) AS sunset_t_avg_4m
        ,CAST(DATEADD(SECOND, sunset_t_avg_6m, '00:00:00') AS TIME(0)) AS sunset_t_avg_6m
    FROM rolling_averages
),

unit_conversion AS (
    SELECT
        city_nm
        ,country_cd
        ,month_dt
        ,temperature_c_avg_2m 
        ,temperature_c_avg_4m 
        ,temperature_c_avg_6m
        ,temperature_c_avg_2m * {{ var('cels_to_fahr') }} AS temperature_f_avg_2m
        ,temperature_c_avg_4m * {{ var('cels_to_fahr') }} AS temperature_f_avg_4m
        ,temperature_c_avg_6m * {{ var('cels_to_fahr') }} AS temperature_f_avg_6m
        ,temperature_c_avg_2m * {{ var('cels_to_kel') }} AS temperature_k_avg_2m
        ,temperature_c_avg_4m * {{ var('cels_to_kel') }} AS temperature_k_avg_4m
        ,temperature_c_avg_6m * {{ var('cels_to_kel') }} AS temperature_k_avg_6m
        ,cloud_coverage_avg_2m
        ,cloud_coverage_avg_4m
        ,cloud_coverage_avg_6m
        ,precipitation_mm_avg_2m
        ,precipitation_mm_avg_4m
        ,precipitation_mm_avg_6m
        ,humidity_avg_2m
        ,humidity_avg_4m
        ,humidity_avg_6m
        ,gust_kph_avg_2m
        ,gust_kph_avg_4m
        ,gust_kph_avg_6m
        ,gust_kph_avg_2m * {{ var('km_to_mi') }} AS gust_miph_avg_2m
        ,gust_kph_avg_4m * {{ var('km_to_mi') }} AS gust_miph_avg_4m
        ,gust_kph_avg_6m * {{ var('km_to_mi') }} AS gust_miph_avg_6m
        ,heatindex_c_no_avg_2m
        ,heatindex_c_no_avg_4m
        ,heatindex_c_no_avg_6m
        ,heatindex_c_no_avg_2m * {{ var('cels_to_fahr') }} AS heatindex_f_avg_2m
        ,heatindex_c_no_avg_4m * {{ var('cels_to_fahr') }} AS heatindex_f_avg_4m
        ,heatindex_c_no_avg_6m * {{ var('cels_to_fahr') }} AS heatindex_f_avg_6m
        ,heatindex_c_no_avg_2m * {{ var('cels_to_kel') }} AS heatindex_k_avg_2m
        ,heatindex_c_no_avg_4m * {{ var('cels_to_kel') }} AS heatindex_k_avg_4m
        ,heatindex_c_no_avg_6m * {{ var('cels_to_kel') }} AS heatindex_k_avg_6m
        ,wind_kph_no_avg_2m
        ,wind_kph_no_avg_4m
        ,wind_kph_no_avg_6m
        ,wind_kph_no_avg_2m * {{ var('km_to_mi') }} AS wind_miph_no_avg_2m
        ,wind_kph_no_avg_4m * {{ var('km_to_mi') }} AS wind_miph_no_avg_4m
        ,wind_kph_no_avg_6m * {{ var('km_to_mi') }} AS wind_miph_no_avg_6m
        ,windchill_c_no_avg_2m
        ,windchill_c_no_avg_4m
        ,windchill_c_no_avg_6m
        ,windchill_c_no_avg_2m * {{ var('cels_to_fahr') }} AS windchill_f_no_avg_2m
        ,windchill_c_no_avg_4m * {{ var('cels_to_fahr') }} AS windchill_f_no_avg_4m
        ,windchill_c_no_avg_6m * {{ var('cels_to_fahr') }} AS windchill_f_no_avg_6m
        ,windchill_c_no_avg_2m * {{ var('cels_to_kel') }} AS windchill_k_no_avg_2m
        ,windchill_c_no_avg_4m * {{ var('cels_to_kel') }} AS windchill_k_no_avg_4m
        ,windchill_c_no_avg_6m * {{ var('cels_to_kel') }} AS windchill_k_no_avg_6m
        ,moonrise_t_avg_2m
        ,moonrise_t_avg_4m
        ,moonrise_t_avg_6m
        ,moonset_t_avg_2m
        ,moonset_t_avg_4m
        ,moonset_t_avg_6m
        ,sunrise_t_avg_2m
        ,sunrise_t_avg_4m
        ,sunrise_t_avg_6m
        ,sunset_t_avg_2m
        ,sunset_t_avg_4m
        ,sunset_t_avg_6m
    FROM rolling_averages
)

{# reorder AS (
SELECT
    city_nm
    ,country_cd
    ,month_dt
    ,temperature_c_avg_2m
    ,temperature_c_avg_4m
    ,temperature_c_avg_6m
    ,cloud_coverage_avg_2m
    ,cloud_coverage_avg_4m
    ,cloud_coverage_avg_6m
    ,precipitation_mm_avg_2m
    ,precipitation_mm_avg_4m
    ,precipitation_mm_avg_6m
    ,humidity_avg_2m
    ,humidity_avg_4m
    ,humidity_avg_6m
    ,gust_kph_avg_2m
    ,gust_kph_avg_4m
    ,gust_kph_avg_6m
    ,heatindex_c_no_avg_2m
    ,heatindex_c_no_avg_4m
    ,heatindex_c_no_avg_6m
    ,wind_kph_no_avg_2m
    ,wind_kph_no_avg_4m
    ,wind_kph_no_avg_6m
    ,windchill_c_no_avg_2m
    ,windchill_c_no_avg_4m
    ,windchill_c_no_avg_6m
    ,moonrise_t_avg_2m
    ,moonrise_t_avg_4m
    ,moonrise_t_avg_6m
    ,moonset_t_avg_2m
    ,moonset_t_avg_4m
    ,moonset_t_avg_6m
    ,sunrise_t_avg_2m
    ,sunrise_t_avg_4m
    ,sunrise_t_avg_6m
    ,sunset_t_avg_2m
    ,sunset_t_avg_4m
    ,sunset_t_avg_6m
FROM rolling_averages 
)#}

SELECT * FROM rolling_averages
