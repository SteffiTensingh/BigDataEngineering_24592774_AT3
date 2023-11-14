{{ config(
    unique_key='lga_code'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_nsw_lga_code
)

 
SELECT * FROM source