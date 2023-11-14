{{
    config(
        unique_key='listing_unique_key'
    )
}}

with source as (
    select * from {{ ref('DIM_Availability') }}
),

renamed as (
    select
        CAST(LISTING_ID AS TEXT) || 
        CAST(SCRAPED_DATE AS TEXT) as listing_unique_key    ,
        LISTING_ID                                          ,
        CAST(SCRAPED_DATE AS DATE) as scraped_date          ,
        HAS_AVAILABILITY                                    ,
        AVAILABILITY_30                                     ,
        dbt_scd_id                                          ,
        case when dbt_valid_from = (select 
                                            min(dbt_valid_from) 
                                    from 
                                            source
                                    ) 
                                    then '1900-01-01'::timestamp 
            else dbt_valid_from end as dbt_valid_from       ,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown'           as listing_unique_key                     ,
        0                   as LISTING_ID                             ,
        '1900-01-01'::DATE  as scraped_date                           ,
        'f'                 as HAS_AVAILABILITY                       , -- False f is replaced
        0 as AVAILABILITY_30                                          ,  -- 0 days are replaced
        'unknown' as dbt_scd_id                                       ,
        '1900-01-01'::timestamp  as dbt_valid_from                    ,
        null::timestamp as dbt_valid_to
)


select * from unknown
union all
select * from renamed
