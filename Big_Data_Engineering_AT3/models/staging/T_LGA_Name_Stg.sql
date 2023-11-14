{{ config(
    unique_key='lga_name'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_nsw_lga_suburb
),
 
unknown  AS (
    SELECT
        'unknown'       AS LGA_NAME     ,
        'unknown'       AS LGA_SUBURB
    FROM source
)
 
SELECT * FROM unknown