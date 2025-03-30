-- Auto Generated (Do not modify) FB7E97E4D1108B09FD67D5C3EEE4C7C047F818421709C3EBC31E6EA3B942FD61
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