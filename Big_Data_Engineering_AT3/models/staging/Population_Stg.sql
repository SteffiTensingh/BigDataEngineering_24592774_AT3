-- staging/g01_nsw_lga_stg.sql

{{
    config(
        unique_key='lga_code_2016'
    )
}}

with source as (
    select * from {{ ref('DIM_Population') }}
),

renamed as (
    select
        CAST(lga_code_2016      AS TEXT) || 
        CAST(dbt_valid_from     AS TEXT)                as listing_unique_key       ,
        CAST(lga_code_2016      AS TEXT)                as lga_code_2016            ,
        CAST(dbt_valid_from     AS DATE)                as scraped_date             ,
        tot_p_m                                                                     ,
        tot_p_f                                                                     ,
        tot_p_p                                                                     ,
        dbt_scd_id                                                                  , 
        CASE
            WHEN dbt_valid_from = (select 
                                        min(dbt_valid_from) 
                                    from 
                                        source) 
            THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from
        END as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown'                   as listing_unique_key,
        '0'                         as lga_code_2016,
        '1900-01-01'::DATE          as scraped_date,
        0                           as tot_p_m,
        0                           as tot_p_f,
        0                           as tot_p_p,
        'unknown'                   as dbt_scd_id,
        '1900-01-01'::timestamp     as dbt_valid_from,
        null::timestamp             as dbt_valid_to
)

select * from unknown
union all
select * from renamed
