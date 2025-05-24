CREATE TABLE [dbt_dq].[dq_business_tests] (

	[m_valid_dt] date NULL, 
	[country_cd] varchar(2) NULL, 
	[table_nm] varchar(22) NOT NULL, 
	[test_type_cd] varchar(15) NOT NULL, 
	[tested_field_nm] varchar(17) NOT NULL, 
	[description] varchar(83) NOT NULL, 
	[failed_record_cnt] int NULL
);