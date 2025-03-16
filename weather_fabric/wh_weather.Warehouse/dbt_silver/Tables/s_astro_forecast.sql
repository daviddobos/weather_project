CREATE TABLE [dbt_silver].[s_astro_forecast] (

	[astro_forecast_pk] varchar(256) NULL, 
	[forecast_dt] datetime2(6) NULL, 
	[city_nm] varchar(50) NULL, 
	[country_nm] varchar(50) NULL, 
	[moon_up_flg] bit NULL, 
	[sun_up_flg] bit NULL, 
	[moon_illumination_no] int NULL, 
	[moon_phase_cd] varchar(20) NULL, 
	[moonrise_t] time(0) NULL, 
	[moonset_t] time(0) NULL, 
	[sunrise_t] time(0) NULL, 
	[sunset_t] time(0) NULL, 
	[p_load_dt] datetime2(6) NULL
);

