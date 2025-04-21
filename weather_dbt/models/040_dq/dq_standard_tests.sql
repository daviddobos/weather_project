WITH s_dim_city_not_null_city_pk_base AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,'s_dim_city' AS table_nm
        ,'not_null' AS test_type_cd
        ,'city_pk' AS tested_field_nm
        ,'Records where the "city_pk" is null' AS description
    FROM {{ ref('s_dim_city') }}
),

s_dim_city_not_null_city_pk_cnt AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_dim_city_not_null_city_pk') }}
    GROUP BY 
        country_cd
        ,county_nm
),

s_dim_city_not_null_city_pk_joined AS (
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
    FROM s_dim_city_not_null_city_pk_base AS base
    LEFT JOIN s_dim_city_not_null_city_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_forecast_not_null_astro_forecast_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'not_null' AS test_type_cd
        ,'forecast_pk' AS tested_field_nm
        ,'Records where the "forecast_pk" is null' AS description
    FROM {{ ref('s_fct_astro_forecast') }}
),

s_fct_astro_forecast_not_null_astro_forecast_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_forecast_not_null_astro_forecast_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_forecast_not_null_astro_forecast_pk_joined AS (
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
    FROM s_fct_astro_forecast_not_null_astro_forecast_pk_base AS base
    LEFT JOIN s_fct_astro_forecast_not_null_astro_forecast_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_not_null_astro_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'not_null' AS test_type_cd
        ,'astro_pk' AS tested_field_nm
        ,'Records where the "astro_pk" is null' AS description
    FROM {{ ref('s_fct_astro') }}
),

s_fct_astro_not_null_astro_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_not_null_astro_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_not_null_astro_pk_joined AS (
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
    FROM s_fct_astro_not_null_astro_pk_base AS base
    LEFT JOIN s_fct_astro_not_null_astro_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_not_null_weather_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'not_null' AS test_type_cd
        ,'weather_pk' AS tested_field_nm
        ,'Records where the "weather_pk" is null' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_not_null_weather_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_not_null_weather_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_not_null_weather_pk_joined AS (
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
    FROM s_fct_weather_forecast_not_null_weather_pk_base AS base
    LEFT JOIN s_fct_weather_forecast_not_null_weather_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_not_null_weather_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'not_null' AS test_type_cd
        ,'weather_pk' AS tested_field_nm
        ,'Records where the "weather_pk" is null' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_not_null_weather_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_not_null_weather_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_not_null_weather_pk_joined AS (
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
    FROM s_fct_weather_not_null_weather_pk_base AS base
    LEFT JOIN s_fct_weather_not_null_weather_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_dim_city_unique_city_pk_base AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,'s_dim_city' AS table_nm
        ,'unique' AS test_type_cd
        ,'city_pk' AS tested_field_nm
        ,'Records where the "city_pk" is duplicated' AS description
    FROM {{ ref('s_dim_city') }}
),

s_dim_city_unique_city_pk_cnt AS (
    SELECT
        NULL AS m_valid_dt
        ,country_cd
        ,county_nm
        ,NULL AS forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_dim_city_unique_city_pk') }}
    GROUP BY 
        country_cd
        ,county_nm
),

s_dim_city_unique_city_pk_joined AS (
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
    FROM s_dim_city_unique_city_pk_base AS base
    LEFT JOIN s_dim_city_unique_city_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),


s_fct_astro_forecast_unique_astro_forecast_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro_forecast' AS table_nm
        ,'unique' AS test_type_cd
        ,'astro_forecast_pk' AS tested_field_nm
        ,'Records where the "astro_forecast_pk" is duplicated' AS description
    FROM {{ ref('s_fct_astro_forecast') }}
),

s_fct_astro_forecast_unique_astro_forecast_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_forecast_unique_astro_forecast_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_forecast_unique_astro_forecast_pk_joined AS (
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
    FROM s_fct_astro_forecast_unique_astro_forecast_pk_base AS base
    LEFT JOIN s_fct_astro_forecast_unique_astro_forecast_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_astro_unique_astro_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'unique' AS test_type_cd
        ,'astro_pk' AS tested_field_nm
        ,'Records where the "astro_pk" is duplicated' AS description
    FROM {{ ref('s_fct_astro') }}
),

s_fct_astro_unique_astro_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_unique_astro_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_unique_astro_pk_joined AS (
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
    FROM s_fct_astro_unique_astro_pk_base AS base
    LEFT JOIN s_fct_astro_unique_astro_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_unique_weather_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'unique' AS test_type_cd
        ,'weather_pk' AS tested_field_nm
        ,'Records where the "weather_pk" is duplicated' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_unique_weather_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_unique_weather_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_unique_weather_pk_joined AS (
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
    FROM s_fct_weather_forecast_unique_weather_pk_base AS base
    LEFT JOIN s_fct_weather_forecast_unique_weather_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_unique_weather_pk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'unique' AS test_type_cd
        ,'weather_pk' AS tested_field_nm
        ,'Records where the "weather_pk" is duplicated' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_unique_weather_pk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_unique_weather_pk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_unique_weather_pk_joined AS (
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
    FROM s_fct_weather_unique_weather_pk_base AS base
    LEFT JOIN s_fct_weather_unique_weather_pk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
), 

s_fct_astro_forecast_rel_s_dim_city_city_fk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro_forecast' AS table_nm
        ,'relationships' AS test_type_cd
        ,'city_fk' AS tested_field_nm
        ,'Records where the "city_fk" does not have a matching entry in the "s_dim_city" dimension table' AS description
    FROM {{ ref('s_fct_astro_forecast') }}
),

s_fct_astro_forecast_rel_s_dim_city_city_fk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_forecast_rel_s_dim_city_city_fk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_forecast_rel_s_dim_city_city_fk_joined AS (
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
    FROM s_fct_astro_forecast_rel_s_dim_city_city_fk_base AS base
    LEFT JOIN s_fct_astro_forecast_rel_s_dim_city_city_fk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),


s_fct_astro_rel_s_dim_city_city_fk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_astro' AS table_nm
        ,'relationships' AS test_type_cd
        ,'city_fk' AS tested_field_nm
        ,'Records where the "city_fk" does not have a matching entry in the "s_dim_city" dimension table' AS description
    FROM {{ ref('s_fct_astro') }}
),

s_fct_astro_rel_s_dim_city_city_fk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_astro_rel_s_dim_city_city_fk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_astro_rel_s_dim_city_city_fk_joined AS (
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
    FROM s_fct_astro_rel_s_dim_city_city_fk_base AS base
    LEFT JOIN s_fct_astro_rel_s_dim_city_city_fk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_forecast_rel_s_dim_city_city_fk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather_forecast' AS table_nm
        ,'relationships' AS test_type_cd
        ,'city_fk' AS tested_field_nm
        ,'Records where the "city_fk" does not have a matching entry in the "s_dim_city" dimension table' AS description
    FROM {{ ref('s_fct_weather_forecast') }}
),

s_fct_weather_forecast_rel_s_dim_city_city_fk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_forecast_rel_s_dim_city_city_fk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_forecast_rel_s_dim_city_city_fk_joined AS (
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
    FROM s_fct_weather_forecast_rel_s_dim_city_city_fk_base AS base
    LEFT JOIN s_fct_weather_forecast_rel_s_dim_city_city_fk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

s_fct_weather_rel_s_dim_city_city_fk_base AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,'s_fct_weather' AS table_nm
        ,'relationships' AS test_type_cd
        ,'city_fk' AS tested_field_nm
        ,'Records where the "city_fk" does not have a matching entry in the "s_dim_city" dimension table' AS description
    FROM {{ ref('s_fct_weather') }}
),

s_fct_weather_rel_s_dim_city_city_fk_cnt AS (
    SELECT
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
        ,COUNT(*) AS failed_record_cnt
    FROM {{ source('dbt_dq_test', 's_fct_weather_rel_s_dim_city_city_fk') }}
    GROUP BY 
        m_valid_dt
        ,country_cd
        ,county_nm
        ,forecast_dt
),

s_fct_weather_rel_s_dim_city_city_fk_joined AS (
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
    FROM s_fct_weather_rel_s_dim_city_city_fk_base AS base
    LEFT JOIN s_fct_weather_rel_s_dim_city_city_fk_cnt AS cnt_records
    ON base.m_valid_dt = cnt_records.m_valid_dt
    AND base.country_cd = cnt_records.country_cd
    AND base.county_nm = cnt_records.county_nm
    AND base.forecast_dt = cnt_records.forecast_dt
),

union_tests AS (
    SELECT * FROM s_dim_city_not_null_city_pk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_forecast_not_null_astro_forecast_pk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_not_null_astro_pk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_not_null_weather_pk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_not_null_weather_pk_joined
    UNION ALL
    SELECT * FROM s_dim_city_unique_city_pk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_forecast_unique_astro_forecast_pk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_unique_astro_pk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_unique_weather_pk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_unique_weather_pk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_forecast_rel_s_dim_city_city_fk_joined
    UNION ALL
    SELECT * FROM s_fct_astro_rel_s_dim_city_city_fk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_forecast_rel_s_dim_city_city_fk_joined
    UNION ALL
    SELECT * FROM s_fct_weather_rel_s_dim_city_city_fk_joined
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