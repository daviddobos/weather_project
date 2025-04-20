CREATE TABLE [dbt_dq].[dq_business_tests_summary] (

	[m_valid_dt] date NULL, 
	[test_type_cd] varchar(15) NOT NULL, 
	[test_cnt] int NULL, 
	[failed_test_cnt] int NULL, 
	[passed_test_cnt] int NULL, 
	[failed_records_cnt] int NULL
);