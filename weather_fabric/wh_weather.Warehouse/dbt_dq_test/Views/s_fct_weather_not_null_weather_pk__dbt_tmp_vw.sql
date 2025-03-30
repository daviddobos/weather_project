-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_not_null_weather_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_weather"
    WHERE weather_pk IS NULL
)

select * from validation

;