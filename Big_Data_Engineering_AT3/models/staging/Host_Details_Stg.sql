{{
    config(
        unique_key='listing_unique_key'
    )
}}

with source as (
    select * from {{ ref('DIM_Host_Details') }}
),

renamed as (
    select
        CAST(HOST_ID AS TEXT) || 
        CAST(SCRAPED_DATE AS TEXT)              as listing_unique_key                   ,
        CAST(HOST_ID AS TEXT)                   as HOST_ID                              ,
        CAST(SCRAPED_DATE AS DATE)              as scraped_date                         ,
        HOST_NAME                                                                       ,
        HOST_SINCE                                                                      ,
        CAST(HOST_IS_SUPERHOST as BOOLEAN)      as HOST_IS_SUPERHOST                    ,
        HOST_NEIGHBOURHOOD                                                              ,
        dbt_scd_id                                                                      ,
        CASE
            WHEN dbt_valid_from = (
                                    select 
                                            min(dbt_valid_from) 
                                    from 
                                            source) 
            THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from
        END as dbt_valid_from                                                           ,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown'                               as listing_unique_key                   ,
        '0'                                     as HOST_ID                              ,
        '1900-01-01'::DATE                      as scraped_date                         ,
        'Unknown Host'                          as HOST_NAME                            ,
        '1900-01-01'::DATE                      as HOST_SINCE                           ,
        FALSE                                   as HOST_IS_SUPERHOST                    ,
        'Unknown Neighborhood'                  as HOST_NEIGHBOURHOOD                   ,
        'unknown'                               as dbt_scd_id                           ,
        '1900-01-01'::timestamp                 as dbt_valid_from                       ,
        null::timestamp                         as dbt_valid_to
)

select * from unknown
union all
select * from renamed
