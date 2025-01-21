WITH country_codes AS ( 
    SELECT 'Hungary' AS country_nm, 'HU' AS country_cd
    UNION ALL
    SELECT 'Romania' AS country_nm, 'RO' AS country_cd
)

SELECT * FROM country_codes