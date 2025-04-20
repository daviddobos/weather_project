CREATE TABLE [dbt_dq].[dq_s_fct_astro] (

	[tested_field_nm] varchar(10) NOT NULL, 
	[test_type_cd] varchar(15) NOT NULL, 
	[m_valid_dt] date NULL, 
	[astro_pk] varchar(256) NULL, 
	[forecast_dt] datetime2(6) NULL, 
	[city_nm] varchar(50) NULL, 
	[city_fk] varchar(256) NULL, 
	[country_cd] varchar(2) NOT NULL, 
	[country_nm] varchar(50) NULL, 
	[county_nm] varchar(23) NULL, 
	[moon_illumination_pct] int NULL, 
	[moon_phase_cd] varchar(20) NULL, 
	[moonrise_t] varchar(10) NULL, 
	[moonset_t] varchar(10) NULL, 
	[sunrise_t] varchar(10) NULL, 
	[sunset_t] varchar(10) NULL, 
	[m_extracted_at_dttm] datetime2(6) NULL, 
	[m_updated_at_dttm] datetime2(6) NULL
);