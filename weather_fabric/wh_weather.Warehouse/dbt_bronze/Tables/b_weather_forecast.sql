CREATE TABLE [dbt_bronze].[b_weather_forecast] (

	[m_valid_dt] date NULL, 
	[rain_chance_pct] int NULL, 
	[snow_chance_pct] int NULL, 
	[cloud_coverage_pct] int NULL, 
	[temp_feelslike_no] decimal(14,4) NULL, 
	[gust_kph_no] decimal(14,4) NULL, 
	[humidity_pct] int NULL, 
	[heatindex_c_no] decimal(14,4) NULL, 
	[is_day_flg] bit NULL, 
	[precip_mm_no] decimal(14,4) NULL, 
	[pressure_mb_no] decimal(14,4) NULL, 
	[temp_c_no] decimal(14,4) NULL, 
	[date_dtt] datetime2(6) NULL, 
	[time_epoch] int NULL, 
	[uv_no] decimal(14,4) NULL, 
	[vis_km_no] decimal(14,4) NULL, 
	[wind_dir_cd] varchar(8) NULL, 
	[wind_kph_no] decimal(14,4) NULL, 
	[windchill_c_no] decimal(14,4) NULL, 
	[city_nm] varchar(30) NULL, 
	[country_nm] varchar(30) NULL, 
	[forecast_dt] date NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);

