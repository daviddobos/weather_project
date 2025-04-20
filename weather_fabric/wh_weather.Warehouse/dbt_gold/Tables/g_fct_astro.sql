CREATE TABLE [dbt_gold].[g_fct_astro] (

	[m_valid_dt] date NULL, 
	[astro_pk] varchar(256) NULL, 
	[forecast_dt] date NULL, 
	[city_nm] varchar(50) NULL, 
	[city_fk] varchar(256) NULL, 
	[country_cd] varchar(2) NOT NULL, 
	[country_nm] varchar(50) NULL, 
	[county_nm] varchar(23) NULL, 
	[moon_illumination_pct] int NULL, 
	[moon_phase_cd] varchar(20) NULL, 
	[moonrise_t] time(0) NULL, 
	[moonset_t] time(0) NULL, 
	[sunrise_t] time(0) NULL, 
	[sunset_t] time(0) NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);