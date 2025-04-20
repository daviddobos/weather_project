-- Auto Generated (Do not modify) DBE61001AF1874CE72B39CCC79DA43201B984A60903E4CC9516B04B746C00161
create view "dbt_dq_test"."s_fct_astro_unique_astro_pk__dbt_tmp_vw" as 
    with validation as (
        -- Find all values of the column that appear more than once
        select astro_pk
        from "wh_weather"."dbt_silver"."s_fct_astro"
        group by astro_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro"
    where astro_pk in (select astro_pk from validation)

;