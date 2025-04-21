WITH s_fct_weather_not_null_weather_pk AS (
    SELECT
        'weather_pk' AS tested_field_nm
        ,'not_null' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_not_null_weather_pk') }}
),

s_fct_weather_unique_weather_pk AS (
    SELECT
        'weather_pk' AS tested_field_nm
        ,'unique' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_unique_weather_pk') }}
),

s_fct_weather_accepted_range_m_valid_dt AS (
    SELECT
        'm_valid_dt' AS tested_field_nm
        ,'accepted_range' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_range_m_valid_dt') }}
),

s_fct_weather_accepted_range_temp_c_no AS (
    SELECT
        'temp_c_no' AS tested_field_nm
        ,'accepted_range' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_range_temp_c_no') }}
),

s_fct_weather_accepted_values_country_cd AS (
    SELECT
        'temp_c_no' AS tested_field_nm
        ,'accepted_range' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_accepted_range_temp_c_no') }}
),

s_fct_weather_invalid_cloud_coverage_cd AS (
    SELECT
        'cloud_coverage_cd' AS tested_field_nm
        ,'invalid' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_cloud_coverage_cd') }}
),

s_fct_weather_invalid_humidity_cd AS (
    SELECT
        'humidity_cd' AS tested_field_nm
        ,'invalid' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_humidity_cd') }}
),

s_fct_weather_invalid_rain_chance_cd AS (
    SELECT
        'rain_chance_cd' AS tested_field_nm
        ,'invalid' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_rain_chance_cd') }}
),

s_fct_weather_invalid_snow_chance_cd AS (
    SELECT
        'snow_chance_cd' AS tested_field_nm
        ,'invalid' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_snow_chance_cd') }}
),

s_fct_weather_invalid_temp_feelslike_cd AS (
    SELECT
        'temp_feelslike_cd' AS tested_field_nm
        ,'invalid' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_invalid_temp_feelslike_cd') }}
),

s_fct_weather_rel_s_dim_city_city_fk AS (
    SELECT
        'city_fk' AS tested_field_nm
        ,'relationships' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test', 's_fct_weather_rel_s_dim_city_city_fk') }}
),

union_tests AS (
    SELECT * FROM s_fct_weather_not_null_weather_pk
    UNION ALL
    SELECT * FROM s_fct_weather_unique_weather_pk
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_range_m_valid_dt
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_range_temp_c_no
    UNION ALL
    SELECT * FROM s_fct_weather_accepted_values_country_cd
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_cloud_coverage_cd
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_humidity_cd
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_rain_chance_cd
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_snow_chance_cd
    UNION ALL
    SELECT * FROM s_fct_weather_invalid_temp_feelslike_cd
    UNION ALL
    SELECT * FROM s_fct_weather_rel_s_dim_city_city_fk
)

SELECT * FROM union_tests