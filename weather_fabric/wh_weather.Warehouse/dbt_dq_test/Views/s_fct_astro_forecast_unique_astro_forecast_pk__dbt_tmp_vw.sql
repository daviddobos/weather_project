-- Auto Generated (Do not modify) DE55D4DEBEBE9D00616E04549AC294BCC59F4896E2D18E65D89C8DC9B9E48063
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