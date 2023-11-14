-- Dim_Suburb

{{
    config(
        unique_key='Suburb_Q_Key'
    )
}}

with source as (
    select
        CAST(LGA_NAME AS TEXT) 
        || CAST(dbt_valid_from AS TEXT)         as Suburb_Q_Key             ,    
        LGA_NAME                                                            ,
        suburb_name                                                         ,
        dbt_scd_id                                                          ,
        dbt_updated_at                                                      ,
        dbt_valid_from                                                      ,
        dbt_valid_to
    from {{ ref('DIM_LGA_Suburb') }}
),

renamed as (
    select
        CAST(LGA_NAME AS TEXT) || 
        CAST(dbt_valid_from AS TEXT)            as Suburb_Q_Key             ,    
        LGA_NAME                                                            ,
        suburb_name                                                         ,
        dbt_scd_id                                                          ,
        dbt_updated_at                                                      ,
        dbt_valid_from                                                      ,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown'                               as Suburb_Q_Key              ,
        'Unknown'                               as lga_name                  ,
        'unknown'                               as suburb_name               ,
        'unknown'                               as dbt_scd_id                ,
        '1900-01-01'::timestamp                 as dbt_updated_at            ,
        '1900-01-01'::timestamp                 as dbt_valid_from            ,
        null::timestamp                         as dbt_valid_to
    )

select * from unknown
union all
select * from renamed
