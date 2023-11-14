{{ config(
    unique_key='LISTING_ID'
)}}
 
WITH source AS (
    SELECT *
    FROM raw.table_listings 
),
 
unknown AS (
    SELECT
            0                   AS LISTING_ID                   ,
            0                   AS SCRAPE_ID                    ,
        '1900-01-01'            AS SCRAPED_DATE                 ,
            0                   AS HOST_ID                      ,
            0                   AS ACCOMMODATES                 ,
            0                   AS PRICE                        ,
        'unknown'               AS HAS_AVAILABILITY             ,
        0                       AS AVAILABILITY_30              ,
        0                       AS NUMBER_OF_REVIEWS            ,
        0                       AS REVIEW_SCORES_RATING         ,
        0                       AS REVIEW_SCORES_ACCURACY       ,
        0                       AS REVIEW_SCORES_CLEANLINESS    ,
        0                       AS REVIEW_SCORES_CHECKIN        ,
        0                       AS REVIEW_SCORES_COMMUNICATION  ,
        0                       AS REVIEW_SCORES_VALUE
    FROM source
)
 
SELECT * FROM unknown