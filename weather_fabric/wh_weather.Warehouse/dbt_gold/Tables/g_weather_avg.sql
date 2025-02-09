CREATE TABLE [dbt_gold].[g_weather_avg] (

	[city_nm] varchar(30) NULL, 
	[country_cd] varchar(2) NOT NULL, 
	[month_dt] varchar(50) NULL, 
	[temperature_c_avg_2m] float NULL, 
	[cloud_coverage_avg_2m] int NULL, 
	[precipitation_mm_avg_2m] float NULL, 
	[humidity_avg_2m] int NULL, 
	[gust_kph_avg_2m] float NULL, 
	[heatindex_c_no_avg_2m] float NULL, 
	[wind_kph_no_avg_2m] float NULL, 
	[windchill_c_no_avg_2m] float NULL, 
	[moonrise_t_avg_2m] int NULL, 
	[moonset_t_avg_2m] int NULL, 
	[sunset_t_avg_2m] int NULL, 
	[sunrise_t_avg_2m] int NULL, 
	[temperature_c_avg_4m] float NULL, 
	[cloud_coverage_avg_4m] int NULL, 
	[precipitation_mm_avg_4m] float NULL, 
	[humidity_avg_4m] int NULL, 
	[gust_kph_avg_4m] float NULL, 
	[heatindex_c_no_avg_4m] float NULL, 
	[wind_kph_no_avg_4m] float NULL, 
	[windchill_c_no_avg_4m] float NULL, 
	[moonrise_t_avg_4m] int NULL, 
	[moonset_t_avg_4m] int NULL, 
	[sunset_t_avg_4m] int NULL, 
	[sunrise_t_avg_4m] int NULL, 
	[temperature_c_avg_6m] float NULL, 
	[cloud_coverage_avg_6m] int NULL, 
	[precipitation_mm_avg_6m] float NULL, 
	[humidity_avg_6m] int NULL, 
	[gust_kph_avg_6m] float NULL, 
	[heatindex_c_no_avg_6m] float NULL, 
	[wind_kph_no_avg_6m] float NULL, 
	[windchill_c_no_avg_6m] float NULL, 
	[moonrise_t_avg_6m] int NULL, 
	[moonset_t_avg_6m] int NULL, 
	[sunset_t_avg_6m] int NULL, 
	[sunrise_t_avg_6m] int NULL
);

