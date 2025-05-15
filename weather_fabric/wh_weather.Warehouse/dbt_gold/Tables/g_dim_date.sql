CREATE TABLE [dbt_gold].[g_dim_date] (

	[date] date NULL, 
	[date_dttm] datetime2(6) NULL, 
	[year_num] int NULL, 
	[year_txt] varchar(4) NULL, 
	[quarter_num] varchar(1) NULL, 
	[quarter_txt] varchar(2) NULL, 
	[month_num] varchar(2) NULL, 
	[month_txt] varchar(10) NULL, 
	[day_num] varchar(2) NULL, 
	[day_txt] varchar(10) NULL, 
	[year_quarter_txt] varchar(7) NULL, 
	[year_month_txt] varchar(7) NULL
);