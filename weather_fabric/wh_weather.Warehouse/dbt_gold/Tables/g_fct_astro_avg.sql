CREATE TABLE [dbt_gold].[g_fct_astro_avg] (

	[m_valid_dt] date NULL, 
	[city_nm] varchar(50) NULL, 
	[city_fk] varchar(256) NULL, 
	[county_nm] varchar(50) NULL, 
	[country_cd] varchar(2) NULL, 
	[moonrise_t_avg_3d] time(0) NULL, 
	[moonrise_t_avg_1w] time(0) NULL, 
	[moonrise_t_avg_2w] time(0) NULL, 
	[moonset_t_avg_3d] time(0) NULL, 
	[moonset_t_avg_1w] time(0) NULL, 
	[moonset_t_avg_2w] time(0) NULL, 
	[sunrise_t_avg_3d] time(0) NULL, 
	[sunrise_t_avg_1w] time(0) NULL, 
	[sunrise_t_avg_2w] time(0) NULL, 
	[sunset_t_avg_3d] time(0) NULL, 
	[sunset_t_avg_1w] time(0) NULL, 
	[sunset_t_avg_2w] time(0) NULL
);