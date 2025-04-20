-- Auto Generated (Do not modify) DBE61001AF1874CE72B39CCC79DA43201B984A60903E4CC9516B04B746C00161
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