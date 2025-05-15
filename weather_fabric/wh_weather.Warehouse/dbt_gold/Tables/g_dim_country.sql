CREATE TABLE [dbt_gold].[g_dim_country] (

	[country_cd] varchar(2) NULL, 
	[country_nm] varchar(50) NULL, 
	[continent_nm] varchar(50) NULL, 
	[eu_member_flg] bit NULL, 
	[currency_cd] varchar(3) NULL, 
	[iso_alpha_3_cd] varchar(3) NULL
);