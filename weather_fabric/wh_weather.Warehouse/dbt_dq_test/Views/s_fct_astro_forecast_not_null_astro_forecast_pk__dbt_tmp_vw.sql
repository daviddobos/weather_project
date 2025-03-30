-- Auto Generated (Do not modify) DE55D4DEBEBE9D00616E04549AC294BCC59F4896E2D18E65D89C8DC9B9E48063
create view "dbt_dq_test"."s_fct_astro_forecast_not_null_astro_forecast_pk__dbt_tmp_vw" as 

with validation as (
    SELECT
        *
    FROM "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    WHERE astro_forecast_pk IS NULL
)

select * from validation

;