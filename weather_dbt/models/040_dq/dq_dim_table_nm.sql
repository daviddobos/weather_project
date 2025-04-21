WITH dq_standard_tests AS (
    SELECT DISTINCT 
        table_nm
    FROM {{ ref('dq_standard_tests') }}
),

dq_business_tests AS (
    SELECT DISTINCT 
        table_nm
    FROM {{ ref('dq_business_tests') }}
)

SELECT * FROM dq_standard_tests
UNION
SELECT * FROM dq_business_tests


