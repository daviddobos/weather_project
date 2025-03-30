-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_unique_weather_pk__dbt_tmp_vw" as 
    with validation as (
        -- Find all values of the column that appear more than once
        select weather_pk
        from "wh_weather"."dbt_silver"."s_fct_weather"
        group by weather_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_fct_weather"
    where weather_pk in (select weather_pk from validation)

;