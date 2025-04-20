CREATE TABLE [dbt_bronze].[b_astro] (

	[m_valid_dt] date NULL, 
	[moon_illumination_pct] int NULL, 
	[moon_phase_cd] varchar(20) NULL, 
	[moonrise_t] varchar(11) NULL, 
	[moonset_t] varchar(10) NULL, 
	[sunrise_t] varchar(10) NULL, 
	[sunset_t] varchar(10) NULL, 
	[city_nm] varchar(50) NULL, 
	[country_nm] varchar(50) NULL, 
	[forecast_dt] date NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);