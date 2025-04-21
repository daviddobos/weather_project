WITH s_dim_date AS (
    SELECT 
        *
    FROM {{ ref('s_dim_date') }}
),

reorder AS (
    SELECT 
        date
        ,date_dttm
        ,year_num
        ,year_txt
        ,quarter_num
        ,quarter_txt
        ,month_num
        ,month_txt
        ,day_num
        ,day_txt
        ,year_quarter_txt
        ,year_month_txt
    FROM s_dim_date
)

SELECT * FROM reorder