{{ config(
    unique_key='lga_code'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_nsw_lga_code
),
 
unknown  AS (
    SELECT
        0               AS LGA_CODE     ,
        'unknown'       AS LGA_NAME
    FROM source
)
 
SELECT * FROM unknown