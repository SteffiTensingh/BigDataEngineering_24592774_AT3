-- Dimension LGA_Suburb
{% snapshot DIM_LGA_Suburb%}

{{
    config(
        target_schema='raw'                         ,
        strategy='check'                            ,
        unique_key='LGA_name'                       ,
        check_cols=['LGA_name','suburb_name']
    )
}}

SELECT 
   *
FROM 
    {{ source('raw', 'table_nsw_lga_suburb') }}

{% endsnapshot %}
