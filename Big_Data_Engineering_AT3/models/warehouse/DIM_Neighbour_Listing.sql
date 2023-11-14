{{ config(
    unique_key='listing_unique_key'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."Neighbour_Listing_Stg"
)
SELECT * FROM source