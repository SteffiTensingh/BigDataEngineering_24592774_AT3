{{ config(
    unique_key='lga_name'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."T_LGA_Name_Stg"
)
 
 
SELECT * FROM source