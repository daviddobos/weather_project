WITH s_dim_city_not_null_city_pk AS (
    SELECT
        'city_pk' AS tested_field_nm
        ,'not_null' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test','s_dim_city_not_null_city_pk') }}
),


s_dim_city_unique_city_pk AS (
    SELECT
        'city_pk' AS tested_field_nm
        ,'unique' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test','s_dim_city_unique_city_pk') }}
),

s_dim_city_accepted_values_country_cd AS (
    SELECT
        'country_cd' AS tested_field_nm
        ,'accepted_values' AS test_type_cd
        ,*
    FROM {{ source('dbt_dq_test','s_dim_city_accepted_values_country_cd') }}
),

union_tests AS (
    SELECT * FROM s_dim_city_not_null_city_pk
    UNION ALL
    SELECT * FROM s_dim_city_unique_city_pk
    UNION ALL
    SELECT * FROM s_dim_city_accepted_values_country_cd
)

SELECT * FROM union_tests