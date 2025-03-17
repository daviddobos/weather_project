CREATE TABLE [dbt_silver].[s_astro] (

	[m_valid_dt] date NULL, 
	[astro_pk] varchar(256) NULL, 
	[forecast_dt] datetime2(6) NULL, 
	[city_nm] varchar(50) NULL, 
	[country_cd] varchar(2) NOT NULL, 
	[country_nm] varchar(50) NULL, 
	[moon_illumination_no] int NULL, 
	[moon_phase_cd] varchar(20) NULL, 
	[moonrise_t] time(0) NULL, 
	[moonset_t] time(0) NULL, 
	[sunrise_t] time(0) NULL, 
	[sunset_t] time(0) NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);

