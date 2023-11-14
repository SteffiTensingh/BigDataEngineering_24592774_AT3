{{ config(
    unique_key='listing_unique_key'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."Property_Type_Stg"
)
 
 
SELECT * FROM source