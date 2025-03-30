-- Auto Generated (Do not modify) FB7E97E4D1108B09FD67D5C3EEE4C7C047F818421709C3EBC31E6EA3B942FD61
create view "dbt_dq_test"."s_fct_astro_rel_s_dim_city_city_fk__dbt_tmp_vw" as 
    with validation as (

        -- Find all records in the child model where the foreign key does not exist in the parent model
        select child.*
        from "wh_weather"."dbt_silver"."s_fct_astro" as child
        left join "wh_weather"."dbt_silver"."s_dim_city" as parent
        on child.city_fk = parent.city_pk
        where parent.city_pk is null and child.city_fk is not null
    )

    -- Return all records from the child model that have unmatched foreign keys
    select *
    from validation

;