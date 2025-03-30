-- Auto Generated (Do not modify) F20BAB86EC4EEAB10B083D9EC9B86860590A6F64E7BB91CD04776F0DCE280F0A
create view "dbt_dq_test"."s_dim_city_not_null_city_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_dim_city"
    WHERE city_pk IS NULL
)

select * from validation

;