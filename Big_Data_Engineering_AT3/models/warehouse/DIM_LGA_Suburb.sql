{{ config(
    unique_key='Suburb_Q_Key'
)}}

WITH source AS (
    SELECT
        *
    FROM public_staging."LGA_Suburb_Stg"
)
SELECT * FROM source