{{ config(
    unique_key='lga_name'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_nsw_lga_suburb
)
SELECT * FROM source