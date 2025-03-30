-- Auto Generated (Do not modify) FB7E97E4D1108B09FD67D5C3EEE4C7C047F818421709C3EBC31E6EA3B942FD61
create view "dbt_dq_test"."s_fct_astro_not_null_astro_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro"
    WHERE astro_pk IS NULL
)

select * from validation

;