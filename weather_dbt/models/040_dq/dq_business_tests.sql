WITH s_fct_weather_forecast_invalid_cloud_coverage_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'invalid' AS test_type_cd
        ,'cloud_coverage_cd' AS tested_field_nm
        ,'Records where "cloud_coverage_cd" does not appropriately match "cloud_coverage_pct"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}

),

s_fct_weather_forecast_invalid_cloud_coverage_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_invalid_cloud_coverage_cd') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_invalid_cloud_coverage_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_invalid_cloud_coverage_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_invalid_cloud_coverage_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt

),

s_dim_city_accepted_values_country_cd_base AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,'s_dim_city' AS table_nm
        ,'accepted_values' AS test_type_cd
        ,'country_cd' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_dim_city') }}

),

s_dim_city_accepted_values_country_cd_cnt AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_dim_city_accepted_values_country_cd') }}
    GROUP BY
        --m_valid_dt
        country_cd
        ,county_nm
        --,forecast_dt
),

s_dim_city_accepted_values_country_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_dim_city_accepted_values_country_cd_base AS base
    LEFT JOIN s_dim_city_accepted_values_country_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),


s_fct_astro_accepted_range_m_valid_dt_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'m_valid_dt' AS tested_field_nm
        ,'Records where the "m_valid_dt" is out of range' AS description
    FROM {{ ref('s_fct_astro') }}

),

s_fct_astro_accepted_range_m_valid_dt_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_accepted_range_m_valid_dt') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_accepted_range_m_valid_dt_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_astro_accepted_range_m_valid_dt_base AS base
    LEFT JOIN s_fct_astro_accepted_range_m_valid_dt_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_accepted_values_country_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'accepted_values' AS test_type_cd
        ,'country_cd' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_fct_astro') }}

),

s_fct_astro_accepted_values_country_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_accepted_range_m_valid_dt') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_accepted_values_country_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_astro_accepted_values_country_cd_base AS base
    LEFT JOIN s_fct_astro_accepted_values_country_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_forecast_accepted_range_m_valid_dt_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro_forecast' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'m_valid_dt' AS tested_field_nm
        ,'Records where the "m_valid_dt" is out of range' AS description
    FROM {{ ref('s_fct_astro') }}
),

s_fct_astro_forecast_accepted_range_m_valid_dt_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_forecast_accepted_range_m_valid_dt') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_forecast_accepted_range_m_valid_dt_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_astro_forecast_accepted_range_m_valid_dt_base AS base
    LEFT JOIN s_fct_astro_forecast_accepted_range_m_valid_dt_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_forecast_accepted_values_country_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro_forecast' AS table_nm
        ,'accepted_values' AS test_type_cd
        ,'country_cd' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_fct_astro_forecast') }}
),

s_fct_astro_forecast_accepted_values_country_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_forecast_accepted_values_country_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_forecast_accepted_values_country_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_astro_forecast_accepted_values_country_cd_base AS base
    LEFT JOIN s_fct_astro_forecast_accepted_values_country_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_accepted_range_m_valid_dt_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'m_valid_dt' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_accepted_range_m_valid_dt_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_range_m_valid_dt') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt

),

s_fct_weather_accepted_range_m_valid_dt_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_accepted_range_m_valid_dt_base AS base
    LEFT JOIN s_fct_weather_accepted_range_m_valid_dt_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_accepted_range_temp_c_no_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'temp_c_no' AS tested_field_nm
        ,'Records where the "temp_c_no" is out of range' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_accepted_range_temp_c_no_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_range_temp_c_no') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_accepted_range_temp_c_no_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_accepted_range_temp_c_no_base AS base
    LEFT JOIN s_fct_weather_accepted_range_temp_c_no_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_accepted_values_country_cd_base AS (
        SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'accepted_values' AS test_type_cd
        ,'country_cd' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_accepted_values_country_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_values_country_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_accepted_values_country_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_accepted_values_country_cd_base AS base
    LEFT JOIN s_fct_weather_accepted_values_country_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_accepted_range_m_valid_dt_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'m_valid_dt' AS tested_field_nm
        ,'Records where the "m_valid_dt" is out of range' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_accepted_range_m_valid_dt_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_accepted_range_m_valid_dt') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_accepted_range_m_valid_dt_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_accepted_range_m_valid_dt_base AS base
    LEFT JOIN s_fct_weather_forecast_accepted_range_m_valid_dt_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_accepted_range_temp_c_no_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'accepted_range' AS test_type_cd
        ,'temp_c_no' AS tested_field_nm
        ,'Records where the "temp_c_no" is out of range' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_accepted_range_temp_c_no_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_accepted_range_temp_c_no') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_accepted_range_temp_c_no_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_accepted_range_temp_c_no_base AS base
    LEFT JOIN s_fct_weather_forecast_accepted_range_temp_c_no_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_accepted_values_country_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'accepted_values' AS test_type_cd
        ,'country_cd' AS tested_field_nm
        ,'Records where the country_cd is not "HU" or "RO"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_accepted_values_country_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_accepted_values_country_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_accepted_values_country_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_accepted_values_country_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_accepted_values_country_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_invalid_humidity_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'invalid' AS test_type_cd
        ,'humidity_cd' AS tested_field_nm
        ,'Records where "humnidity_cd" does not appropriately match "humidity_pct"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_invalid_humidity_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_invalid_humidity_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_invalid_humidity_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_invalid_humidity_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_invalid_humidity_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_invalid_rain_chance_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'invalid' AS test_type_cd
        ,'rain_chance_cd' AS tested_field_nm
        ,'Records where "rain_chance_cd" does not appropriately match "rain_chance_pct"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_invalid_rain_chance_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_invalid_rain_chance_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_invalid_rain_chance_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_invalid_rain_chance_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_invalid_rain_chance_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_invalid_snow_chance_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'invalid' AS test_type_cd
        ,'snow_chance_cd' AS tested_field_nm
        ,'Records where "snow_chance_cd" does not appropriately match "snow_chance_pct"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_invalid_snow_chance_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_invalid_snow_chance_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_invalid_snow_chance_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_invalid_snow_chance_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_invalid_snow_chance_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_invalid_temp_feelslike_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'invalid' AS test_type_cd
        ,'snow_chance_cd' AS tested_field_nm
        ,'Records where "feels_like_cd" does not appropriately match "feels_like_no"' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_invalid_temp_feelslike_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_invalid_temp_feelslike_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_invalid_temp_feelslike_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_forecast_invalid_temp_feelslike_cd_base AS base
    LEFT JOIN s_fct_weather_forecast_invalid_temp_feelslike_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_invalid_cloud_coverage_cd_base AS (
        SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'invalid' AS test_type_cd
        ,'cloud_coverage_cd' AS tested_field_nm
        ,'Records where "cloud_coverage_cd" does not appropriately match "cloud_coverage_pct"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_invalid_cloud_coverage_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_cloud_coverage_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_invalid_cloud_coverage_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_invalid_cloud_coverage_cd_base AS base
    LEFT JOIN s_fct_weather_invalid_cloud_coverage_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_invalid_humidity_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'invalid' AS test_type_cd
        ,'humidity_cd' AS tested_field_nm
        ,'Records where "humidity_cd" does not appropriately match "humidity_pct"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_invalid_humidity_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_humidity_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_invalid_humidity_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_invalid_humidity_cd_base AS base
    LEFT JOIN s_fct_weather_invalid_humidity_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_invalid_rain_chance_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'invalid' AS test_type_cd
        ,'rain_chance_cd' AS tested_field_nm
        ,'Records where "rain_chance_cd" does not appropriately match "rain_chance_pct"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_invalid_rain_chance_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_rain_chance_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_invalid_rain_chance_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_invalid_rain_chance_cd_base AS base
    LEFT JOIN s_fct_weather_invalid_rain_chance_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_invalid_snow_chance_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'invalid' AS test_type_cd
        ,'snow_chance_cd' AS tested_field_nm
        ,'Records where "snow_chance_cd" does not appropriately match "snow_chance_pct"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_invalid_snow_chance_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_snow_chance_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_invalid_snow_chance_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_invalid_snow_chance_cd_base AS base
    LEFT JOIN s_fct_weather_invalid_snow_chance_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_invalid_temp_feelslike_cd_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'invalid' AS test_type_cd
        ,'temp_feelslike_cd' AS tested_field_nm
        ,'Records where "temp_feelslike_cd" does not appropriately match "temp_feelslike_no"' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_invalid_temp_feelslike_cd_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_temp_feelslike_cd') }}
    GROUP BY
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_invalid_temp_feelslike_cd_joined AS (
    SELECT
        base.m_valid_dt
        ,base.country_cd
        ,base.county_nm
        ,base.forecast_dt
        ,base.table_nm
        ,base.test_type_cd
        ,base.tested_field_nm
        ,base.description
        ,cnt_records.failed_record_cnt
    FROM s_fct_weather_invalid_temp_feelslike_cd_base AS base
    LEFT JOIN s_fct_weather_invalid_temp_feelslike_cd_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

union_tests AS (
    SELECT * FROM s_fct_weather_forecast_invalid_cloud_coverage_cd_joined
    UNION ALL
    SELECT * FROM s_dim_city_accepted_values_country_cd_joined
    UNION ALL
    SELECT * FROM s_fct_astro_accepted_range_m_valid_dt_joined
    UNION ALL
    SELECT * FROM s_fct_astro_accepted_values_country_cd_joined
    UNION ALL
    SELECT * FROM s_fct_astro_forecast_accepted_range_m_valid_dt_joined
    UNION ALL
    SELECT * FROM s_fct_astro_forecast_accepted_values_country_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_range_m_valid_dt_joined
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_range_temp_c_no_joined
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_values_country_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_accepted_range_m_valid_dt_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_accepted_range_temp_c_no_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_accepted_values_country_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_invalid_cloud_coverage_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_invalid_humidity_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_invalid_rain_chance_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_invalid_snow_chance_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_invalid_temp_feelslike_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_cloud_coverage_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_humidity_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_rain_chance_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_snow_chance_cd_joined
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_temp_feelslike_cd_joined
),

failed_record_cnt_fixed AS (
    SELECT 
        m_valid_dt
        ,country_cd
        ,table_nm
        ,test_type_cd
        ,tested_field_nm
        ,description
        ,failed_record_cnt
        ,CASE WHEN failed_record_cnt is NULL THEN 0 ELSE failed_record_cnt END AS failed_record_cnt_fixed
    FROM union_tests
),

reorder AS (
    SELECT 
        m_valid_dt
        ,country_cd
        ,table_nm
        ,test_type_cd
        ,tested_field_nm
        ,description
        ,failed_record_cnt_fixed as failed_record_cnt
    FROM failed_record_cnt_fixed
)

SELECT * FROM reorder