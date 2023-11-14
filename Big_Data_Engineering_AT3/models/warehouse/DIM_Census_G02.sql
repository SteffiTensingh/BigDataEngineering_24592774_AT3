{{ config(
    unique_key='lga_code_2016'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."T_Census_G02_Stg"
)
 
 
SELECT * FROM source