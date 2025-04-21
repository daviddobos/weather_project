WITH ld_date AS (
    SELECT 
        *
    FROM {{ source('landing_delta', 'ld_date') }}
),

b_date AS (
    SELECT
        CONVERT(DATE, date) AS date
        ,CONVERT(DATETIME2(6), date_timestamp) AS date_dttm
        ,CONVERT(INT, year_num) AS year_num
        ,CONVERT(VARCHAR(4), year_txt) AS year_txt
        ,CONVERT(VARCHAR(1), quarter_num) AS quarter_num
        ,CONVERT(VARCHAR(2), quarter_txt) AS quarter_txt
        ,CONVERT(VARCHAR(2), month_num) AS month_num
        ,CONVERT(VARCHAR(10), month_txt) AS month_txt
        ,CONVERT(VARCHAR(2), day_num) AS day_num
        ,CONVERT(VARCHAR(10), day_txt) AS day_txt
        ,CONVERT(VARCHAR(7), year_quarter_txt) AS year_quarter_txt
        ,CONVERT(VARCHAR(7), year_month_txt) AS year_month_txt
    FROM ld_date
)

SELECT * FROM b_date
