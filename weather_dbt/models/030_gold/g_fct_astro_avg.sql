WITH s_fct_astro AS (
    SELECT
        *
    FROM {{ ref('s_fct_astro') }}
),

daily_aggregates AS (
    SELECT
        m_valid_dt
        ,city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        -- Convert TIME to seconds for averaging
        ,AVG(DATEDIFF(SECOND, '00:00:00', moonrise_t)) AS moonrise_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', moonset_t)) AS moonset_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', sunrise_t)) AS sunrise_t_avg
        ,AVG(DATEDIFF(SECOND, '00:00:00', sunset_t)) AS sunset_t_avg
    FROM s_fct_astro
    GROUP BY m_valid_dt, city_nm, city_fk, county_nm, country_cd
),

rolling_averages AS (
    SELECT
        city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,m_valid_dt

        -- Rolling averages (3 days)
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_3d

        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_3d

        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_3d

        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_3d

        -- Rolling averages (1 week)
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_1w

        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_1w

        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_1w

        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_1w

        -- Rolling averages (2 weeks = 14 days)
        ,AVG(moonrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS moonrise_t_avg_2w

        ,AVG(moonset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS moonset_t_avg_2w

        ,AVG(sunset_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sunset_t_avg_2w

        ,AVG(sunrise_t_avg) OVER (
            PARTITION BY city_nm, city_fk, county_nm, country_cd
            ORDER BY m_valid_dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sunrise_t_avg_2w

    FROM daily_aggregates
),

astro_data_to_time AS (
    SELECT
        m_valid_dt
        ,city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,CAST(DATEADD(SECOND, moonrise_t_avg_3d, '00:00:00') AS TIME(0)) AS moonrise_t_avg_3d
        ,CAST(DATEADD(SECOND, moonrise_t_avg_1w, '00:00:00') AS TIME(0)) AS moonrise_t_avg_1w
        ,CAST(DATEADD(SECOND, moonrise_t_avg_2w, '00:00:00') AS TIME(0)) AS moonrise_t_avg_2w
        ,CAST(DATEADD(SECOND, moonset_t_avg_3d, '00:00:00') AS TIME(0)) AS moonset_t_avg_3d
        ,CAST(DATEADD(SECOND, moonset_t_avg_1w, '00:00:00') AS TIME(0)) AS moonset_t_avg_1w
        ,CAST(DATEADD(SECOND, moonset_t_avg_2w, '00:00:00') AS TIME(0)) AS moonset_t_avg_2w
        ,CAST(DATEADD(SECOND, sunrise_t_avg_3d, '00:00:00') AS TIME(0)) AS sunrise_t_avg_3d
        ,CAST(DATEADD(SECOND, sunrise_t_avg_1w, '00:00:00') AS TIME(0)) AS sunrise_t_avg_1w
        ,CAST(DATEADD(SECOND, sunrise_t_avg_2w, '00:00:00') AS TIME(0)) AS sunrise_t_avg_2w
        ,CAST(DATEADD(SECOND, sunset_t_avg_3d, '00:00:00') AS TIME(0)) AS sunset_t_avg_3d
        ,CAST(DATEADD(SECOND, sunset_t_avg_1w, '00:00:00') AS TIME(0)) AS sunset_t_avg_1w
        ,CAST(DATEADD(SECOND, sunset_t_avg_2w, '00:00:00') AS TIME(0)) AS sunset_t_avg_2w
    FROM rolling_averages
),

reorder AS (
    SELECT
        m_valid_dt
        ,city_nm
        ,city_fk
        ,county_nm
        ,country_cd
        ,moonrise_t_avg_3d
        ,moonrise_t_avg_1w
        ,moonrise_t_avg_2w
        ,moonset_t_avg_3d
        ,moonset_t_avg_1w
        ,moonset_t_avg_2w
        ,sunrise_t_avg_3d
        ,sunrise_t_avg_1w
        ,sunrise_t_avg_2w
        ,sunset_t_avg_3d
        ,sunset_t_avg_1w
        ,sunset_t_avg_2w
    FROM astro_data_to_time
)

SELECT * FROM reorder
