-- Auto Generated (Do not modify) FB7E97E4D1108B09FD67D5C3EEE4C7C047F818421709C3EBC31E6EA3B942FD61
create view "dbt_dq_test"."s_fct_astro_accepted_values_country_cd__dbt_tmp_vw" as 

with validation as (
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro"
    where country_cd not in 
        (

        
            'HU'
        
   )
)

select * from validation

;