-- Dimension Review_Scores
{% snapshot DIM_Review_Scores %}

{{
    config(
        target_schema='raw'                 ,
        strategy='timestamp'                ,
        unique_key='listing_unique_key'     ,
        updated_at='SCRAPED_DATE'
    )
}}

SELECT 
    LISTING_ID                              ,
    SCRAPED_DATE                            ,
    REVIEW_SCORES_RATING                    ,
    REVIEW_SCORES_ACCURACY                  ,
    REVIEW_SCORES_CLEANLINESS               ,
    REVIEW_SCORES_CHECKIN                   ,
    REVIEW_SCORES_COMMUNICATION             ,
    REVIEW_SCORES_VALUE                     ,
    CAST(LISTING_ID AS TEXT) || CAST(SCRAPED_DATE AS TEXT) as listing_unique_key
FROM 
    {{ source('raw', 'table_listings') }}

{% endsnapshot %}
