-- Auto Generated (Do not modify) F20BAB86EC4EEAB10B083D9EC9B86860590A6F64E7BB91CD04776F0DCE280F0A
create view "dbt_dq_test"."s_dim_city_accepted_values_country_cd__dbt_tmp_vw" as 

with validation as (
    select *
    from "wh_weather"."dbt_silver"."s_dim_city"
    where country_cd not in 
        (

        
            'HU'
        
   )
)

select * from validation

;