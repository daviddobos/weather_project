-- Auto Generated (Do not modify) DBE61001AF1874CE72B39CCC79DA43201B984A60903E4CC9516B04B746C00161
create view "dbt_dq_test"."s_fct_astro_not_null_astro_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro"
    WHERE astro_pk IS NULL
)

select * from validation

;