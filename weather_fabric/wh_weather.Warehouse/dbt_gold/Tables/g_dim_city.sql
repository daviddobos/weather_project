CREATE TABLE [dbt_gold].[g_dim_city] (

	[city_pk] varchar(256) NULL, 
	[country_cd] varchar(2) NULL, 
	[county_nm] varchar(50) NULL, 
	[city_nm] varchar(50) NULL, 
	[ksh_cd] int NULL, 
	[city_type_cd] varchar(50) NULL, 
	[district_cd] varchar(50) NULL, 
	[district_nm] varchar(50) NULL, 
	[district_seat_nm] varchar(50) NULL, 
	[area_no] decimal(18,0) NULL, 
	[population_no] int NULL, 
	[apartments_no] int NULL, 
	[latitude_no] float NULL, 
	[longitude_no] float NULL
);