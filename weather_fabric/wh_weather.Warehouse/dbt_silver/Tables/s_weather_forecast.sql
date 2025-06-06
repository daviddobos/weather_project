CREATE TABLE [dbt_silver].[s_weather_forecast] (

	[weather_pk] varchar(256) NULL, 
	[forecast_dt] date NULL, 
	[city_nm] varchar(30) NULL, 
	[country_cd] varchar(2) NOT NULL, 
	[country_nm] varchar(30) NULL, 
	[cloud_coverage_no] int NULL, 
	[cloud_coverage_cd] varchar(19) NOT NULL, 
	[gust_kph_no] float NULL, 
	[gust_miph_no] float NULL, 
	[heatindex_c_no] float NULL, 
	[heatindex_f_no] float NULL, 
	[heatindex_k_no] float NULL, 
	[humidity_no] int NULL, 
	[humidity_cd] varchar(9) NOT NULL, 
	[is_day_flg] bit NULL, 
	[precip_mm_no] float NULL, 
	[pressure_mb_no] float NULL, 
	[temp_c_no] float NULL, 
	[temp_f_no] float NULL, 
	[temp_k_no] float NULL, 
	[temp_feelslike_no] float NULL, 
	[temp_feels_like_cd] varchar(9) NOT NULL, 
	[date_dtt] datetime2(6) NULL, 
	[time_epoch] int NULL, 
	[rain_chance_no] int NULL, 
	[rain_chance_cd] varchar(9) NOT NULL, 
	[snow_chance_no] int NULL, 
	[snow_chance_cd] varchar(9) NOT NULL, 
	[uv_no] float NULL, 
	[vis_km_no] float NULL, 
	[vis_mi_no] float NULL, 
	[wind_dir_cd] varchar(8) NULL, 
	[wind_kph_no] float NULL, 
	[wind_miph_no] float NULL, 
	[windchill_c_no] float NULL, 
	[windchill_f_no] float NULL, 
	[windchill_k_no] float NULL, 
	[p_load_dt] datetime2(6) NULL
);

