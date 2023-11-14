{{ config(
    unique_key='LISTING_ID'
)}}
 
WITH source AS (
    SELECT *
    FROM raw.table_listings 
)
SELECT * FROM source