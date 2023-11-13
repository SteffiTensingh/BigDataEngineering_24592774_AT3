-- Dimension Price
{% snapshot DIM_Price%}

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
    PRICE                                   ,
    CAST(LISTING_ID AS TEXT) || CAST(SCRAPED_DATE AS TEXT) as listing_unique_key
FROM 
    {{ source('raw', 'table_listings') }}

{% endsnapshot %}
