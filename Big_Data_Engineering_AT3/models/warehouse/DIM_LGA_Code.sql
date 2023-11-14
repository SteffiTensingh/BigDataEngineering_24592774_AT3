{{ config(
    unique_key='lga_code'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."T_LGA_Code_Stg"
)
 
 
SELECT * FROM source