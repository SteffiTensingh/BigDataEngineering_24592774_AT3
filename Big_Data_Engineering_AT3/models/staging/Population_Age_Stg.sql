with source as (
    select * from {{ ref('DIM_Population_Age') }}
),

renamed as (
    select
        CAST(lga_code_2016  AS TEXT) || 
        CAST(dbt_valid_from AS TEXT)                as listing_unique_key       ,
        CAST(LGA_CODE_2016  AS TEXT)                as lga_code_2016            ,
        age_0_4_yr_m                                                            ,
        age_0_4_yr_f                                                            ,
        age_0_4_yr_p                                                            ,
        age_5_14_yr_m                                                           ,
        age_5_14_yr_f                                                           ,
        age_5_14_yr_p                                                           ,
        age_15_19_yr_m                                                          ,
        age_15_19_yr_f                                                          ,  
        age_15_19_yr_p                                                          ,
        age_20_24_yr_m                                                          ,
        age_20_24_yr_f                                                          ,
        age_20_24_yr_p                                                          ,
        age_25_34_yr_m                                                          ,
        age_25_34_yr_f                                                          ,
        age_25_34_yr_p                                                          ,
        age_35_44_yr_m                                                          ,
        age_35_44_yr_f                                                          ,
        age_35_44_yr_p                                                          ,
        age_45_54_yr_m                                                          ,
        age_45_54_yr_f                                                          ,
        age_45_54_yr_p                                                          ,
        age_55_64_yr_m                                                          ,
        age_55_64_yr_f                                                          ,
        age_55_64_yr_p                                                          ,
        age_65_74_yr_m                                                          ,
        age_65_74_yr_f                                                          ,
        age_65_74_yr_p                                                          ,
        age_75_84_yr_m                                                          ,
        age_75_84_yr_f                                                          ,
        age_75_84_yr_p                                                          ,
        age_85ov_m                                                              ,
        age_85ov_f                                                              ,
        age_85ov_p                                                              ,
        dbt_scd_id                                                              ,
        CASE
            WHEN dbt_valid_from = (
                                    select 
                                            min(dbt_valid_from) 
                                    from source) 
            THEN '1900-01-01'::timestamp
            ELSE dbt_valid_from
        END as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown'       as listing_unique_key                                   ,
        '0'             as lga_code_2016                                        ,     
        0               as age_0_4_yr_m                                         ,
        0               as age_0_4_yr_f                                         ,
        0               as age_0_4_yr_p                                         ,
        0               as age_5_14_yr_m                                        ,
        0               as age_5_14_yr_f                                        ,
        0               as age_5_14_yr_p                                        ,
        0               as age_15_19_yr_m                                       ,
        0               as age_15_19_yr_f                                       ,   
        0               as age_15_19_yr_p                                       ,
        0               as age_20_24_yr_m                                       ,
        0               as age_20_24_yr_f                                       ,
        0               as age_20_24_yr_p                                       ,
        0               as age_25_34_yr_m                                       ,
        0               as age_25_34_yr_f                                       ,
        0               as age_25_34_yr_p                                       ,
        0               as age_35_44_yr_m                                       ,
        0               as age_35_44_yr_f                                       ,
        0               as age_35_44_yr_p                                       ,
        0               as age_45_54_yr_m                                       ,
        0               as age_45_54_yr_f                                       ,
        0               as age_45_54_yr_p                                       ,
        0               as age_55_64_yr_m                                       ,
        0               as age_55_64_yr_f                                       ,
        0               as age_55_64_yr_p                                       ,
        0               as age_65_74_yr_m                                       ,
        0               as age_65_74_yr_f                                       ,
        0               as age_65_74_yr_p                                       ,
        0               as age_75_84_yr_m                                       ,
        0               as age_75_84_yyr_f                                      ,
        0               as age_75_84_yr_p                                       ,
        0               as age_85ov_m                                           ,
        0               as age_85ov_f                                           ,
        0               as age_85ov_p                                           ,
        'unknown'       as dbt_scd_id                                           ,
        '1900-01-01'::timestamp 
                        as dbt_valid_from                                       ,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from renamed
