CREATE TABLE [dbt_silver].[s_dim_city] (

	[city_pk] varchar(256) NULL, 
	[country_cd] varchar(16) NULL, 
	[county_nm] varchar(23) NULL, 
	[city_nm] varchar(23) NULL, 
	[ksh_cd] int NULL, 
	[city_type_cd] varchar(35) NULL, 
	[district_cd] varchar(16) NULL, 
	[district_nm] varchar(21) NULL, 
	[district_seat_nm] varchar(20) NULL, 
	[area_no] int NULL, 
	[population_no] int NULL, 
	[apartments_no] int NULL
);