{{ config(
    unique_key='listing_unique_key'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."Availability_Stg"
)
SELECT * FROM source