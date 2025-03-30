-- Auto Generated (Do not modify) AB41B1863F5F738D50BAE302348B7B3FCF46D062FA26742FF9F340119E78C0CC
create view "dbt_dq_test"."s_fct_weather_accepted_range_temp_c_no__dbt_tmp_vw" as 

with meet_condition as(
  select *
  from "wh_weather"."dbt_silver"."s_fct_weather"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not temp_c_no >= -20
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not temp_c_no <= 50
)

select *
from validation_errors

;