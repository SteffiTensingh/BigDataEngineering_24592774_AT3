{{ config(
    unique_key='lga_code_2016'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_g01_nsw_lga
)
 
 
SELECT * FROM source