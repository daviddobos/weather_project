-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_accepted_values_country_cd__dbt_tmp_vw" as 

with validation as (
    select *
    from "wh_weather"."dbt_silver"."s_fct_weather"
    where country_cd not in 
        (

        
            'HU'
        
   )
)

select * from validation

;