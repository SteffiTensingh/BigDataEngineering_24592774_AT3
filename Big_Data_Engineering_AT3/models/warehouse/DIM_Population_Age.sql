{{ config(
    unique_key='listing_unique_key'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."Population_Age_Stg"
)
SELECT * FROM source