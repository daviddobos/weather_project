-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_rel_s_dim_city_city_fk__dbt_tmp_vw" as 
    with validation as (

        -- Find all records in the child model where the foreign key does not exist in the parent model
        select child.*
        from "wh_weather"."dbt_silver"."s_fct_weather" as child
        left join "wh_weather"."dbt_silver"."s_dim_city" as parent
        on child.city_fk = parent.city_pk
        where parent.city_pk is null and child.city_fk is not null
    )

    -- Return all records from the child model that have unmatched foreign keys
    select *
    from validation

;