-- Dimension Property_Type
{% snapshot DIM_Property_Type %}

{{
    config(
        target_schema='raw'                     ,
        strategy='timestamp'                    ,
        unique_key='listing_unique_key'         ,
        updated_at='SCRAPED_DATE'
    )
}}

SELECT 
    LISTING_ID, PROPERTY_TYPE,
    SCRAPED_DATE,
    CAST(LISTING_ID AS TEXT) || CAST(SCRAPED_DATE AS TEXT) as listing_unique_key
FROM 
    {{ source('raw', 'table_listings') }}

{% endsnapshot %}
