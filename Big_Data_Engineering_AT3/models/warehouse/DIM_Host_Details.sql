{{ 
    config
        (
    unique_key='listing_unique_key'
        )
}}

WITH source AS (
    SELECT
        *
    FROM public_staging."Host_Details_Stg"
) 
SELECT * FROM source