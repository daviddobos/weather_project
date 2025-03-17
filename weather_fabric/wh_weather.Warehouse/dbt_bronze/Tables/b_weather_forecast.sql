CREATE TABLE [dbt_bronze].[b_weather_forecast] (

	[m_valid_dt] date NULL, 
	[rain_chance_no] int NULL, 
	[snow_chance_no] int NULL, 
	[cloud_coverage_no] int NULL, 
	[temp_feelslike_no] float NULL, 
	[gust_kph_no] float NULL, 
	[humidity_no] int NULL, 
	[heatindex_c_no] float NULL, 
	[is_day_flg] bit NULL, 
	[precip_mm_no] float NULL, 
	[pressure_mb_no] float NULL, 
	[temp_c_no] float NULL, 
	[date_dtt] datetime2(6) NULL, 
	[time_epoch] int NULL, 
	[uv_no] float NULL, 
	[vis_km_no] float NULL, 
	[wind_dir_cd] varchar(8) NULL, 
	[wind_kph_no] float NULL, 
	[windchill_c_no] float NULL, 
	[city_nm] varchar(30) NULL, 
	[country_nm] varchar(30) NULL, 
	[forecast_dt] date NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);

