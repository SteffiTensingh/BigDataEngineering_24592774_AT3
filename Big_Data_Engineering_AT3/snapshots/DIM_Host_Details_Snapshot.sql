-- Dimension Host Details
{% snapshot DIM_Host_Details%}

{{
    config(
        target_schema='raw'                     ,
        strategy='timestamp'                    ,
        unique_key='listing_unique_key'         ,
        updated_at='SCRAPED_DATE'
    )
}}

SELECT 
    HOST_ID                                     ,
    SCRAPED_DATE                                ,
    HOST_NAME                                   ,
    HOST_SINCE                                  ,
    HOST_IS_SUPERHOST                           ,
    HOST_NEIGHBOURHOOD                          ,
    CAST(LISTING_ID AS TEXT) || CAST(SCRAPED_DATE AS TEXT) as listing_unique_key
FROM 
    {{ source('raw', 'table_listings') }}

{% endsnapshot %}
