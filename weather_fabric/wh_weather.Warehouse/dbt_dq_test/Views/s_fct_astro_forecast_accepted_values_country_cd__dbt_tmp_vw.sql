-- Auto Generated (Do not modify) DE55D4DEBEBE9D00616E04549AC294BCC59F4896E2D18E65D89C8DC9B9E48063
create view "dbt_dq_test"."s_fct_astro_forecast_accepted_values_country_cd__dbt_tmp_vw" as 

with validation as (
    select *
    from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
    where country_cd not in 
        (

        
            'HU'
        
   )
)

select * from validation

;