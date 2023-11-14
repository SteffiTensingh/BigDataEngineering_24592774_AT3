{{ config(
    unique_key='lga_code_2016'
)}}

WITH source AS (
    SELECT
        *
    FROM raw.table_g02_nsw_lga
),
 
unknown AS (
    SELECT
        CAST(lga_code_2016  AS TEXT) as lga_code_2016       ,
        0       AS median_age_persons                       ,
        0       AS median_mortgage_repay_monthly            ,
        0       AS median_tot_prsnl_inc_weekly              ,
        0       AS median_rent_weekly                       ,
        0       AS median_tot_fam_inc_weekly                ,
        0       AS average_num_psns_per_bedroom             ,
        0       AS median_tot_hhd_inc_weekly                ,
        0.0     AS average_household_size
    FROM source
)
 
SELECT * FROM unknown
