-- Auto Generated (Do not modify) DE55D4DEBEBE9D00616E04549AC294BCC59F4896E2D18E65D89C8DC9B9E48063
create view "dbt_dq_test"."s_fct_astro_forecast_accepted_range_m_valid_dt__dbt_tmp_vw" as 

with meet_condition as(
  select *
  from "wh_weather"."dbt_silver"."s_fct_astro_forecast"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not m_valid_dt >= CAST(DATEFROMPARTS(YEAR(GETDATE()), 1, 1) AS DATE)
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not m_valid_dt <= CAST(DATEFROMPARTS(YEAR(GETDATE()), 12, 31) AS DATE)
)

select *
from validation_errors

;