{{
    config(
        unique_key='listing_unique_key'
    )
}}

WITH source as (
    SELECT * FROM {{ ref('DIM_Host_Details') }}
),

renamed as (
    SELECT
        CAST(HOST_ID AS TEXT) || 
        CAST(SCRAPED_DATE AS TEXT)              as listing_unique_key                   ,
        CAST(HOST_ID AS TEXT)                   as HOST_ID                              ,
        CAST(SCRAPED_DATE AS DATE)              as scraped_date                         ,
        HOST_NAME                                                                       ,
        HOST_SINCE                                                                      ,
        CASE
            WHEN HOST_IS_SUPERHOST = 'NaN' THEN false  -- Replace 'NaN' with false in boolean columns
            ELSE CAST(HOST_IS_SUPERHOST AS BOOLEAN)
        END AS HOST_IS_SUPERHOST,
        HOST_NEIGHBOURHOOD                                                              ,
        dbt_scd_id                                                                      ,
        CASE
            WHEN dbt_valid_from = (
                                    SELECT 
                                            MIN(dbt_valid_from) 
                                    FROM 
                                            source) 
            THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from
        END AS dbt_valid_from                                                           ,
        dbt_valid_to
    FROM source
),

unknown as (
    SELECT
        'unknown'                               as listing_unique_key                   ,
        '0'                                     as HOST_ID                              ,
        '1900-01-01'::DATE                      as scraped_date                         ,
        'Unknown Host'                          as HOST_NAME                            ,
        '1900-01-01'::DATE                      as HOST_SINCE                           ,
        false                                   as HOST_IS_SUPERHOST                    , 
        'Unknown Neighborhood'                  as HOST_NEIGHBOURHOOD                   ,
        'unknown'                               as dbt_scd_id                           ,
        '1900-01-01'::timestamp                 as dbt_valid_from                       ,
        null::timestamp                         as dbt_valid_to
)

SELECT * FROM unknown
UNION ALL
SELECT * FROM renamed
