-- Auto Generated (Do not modify) 87BA6FAC982E4CDBDB48EA047FF910B7D987ED81A26CB731041DD2A84F928624
create view "dbt_dq_test"."s_fct_astro_forecast_not_null_astro_forecast_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    WHERE astro_forecast_pk IS NULL
)

select * from validation

;