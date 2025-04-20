-- Auto Generated (Do not modify) 87BA6FAC982E4CDBDB48EA047FF910B7D987ED81A26CB731041DD2A84F928624
create view "dbt_dq_test"."s_fct_astro_forecast_unique_astro_forecast_pk__dbt_tmp_vw" as 
    with validation as (
        -- Find all values of the column that appear more than once
        select astro_forecast_pk
        from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
        group by astro_forecast_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    where astro_forecast_pk in (select astro_forecast_pk from validation)

;