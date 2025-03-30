-- Auto Generated (Do not modify) F20BAB86EC4EEAB10B083D9EC9B86860590A6F64E7BB91CD04776F0DCE280F0A
create view "dbt_dq_test"."s_dim_city_unique_city_pk__dbt_tmp_vw" as 
    with validation as (
        -- Find all values of the column that appear more than once
        select city_pk
        from "wh_weather"."dbt_silver"."s_dim_city"
        group by city_pk
        having count(*) > 1
    )

    -- Return the full records for rows that have duplicate values
    select *
    from "wh_weather"."dbt_silver"."s_dim_city"
    where city_pk in (select city_pk from validation)

;