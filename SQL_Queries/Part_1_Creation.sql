------ Begining Of part 1_ Question 2------------------

-- Creating schema 'raw'

CREATE SCHEMA raw; 


-- Verifying the existence of Schema 'raw'

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'raw';

-- Census Table Creation 

-- 2016_Census_G01_NSW_LGA

create table if not exists
				raw.Table_G01_NSW_LGA
(
				LGA_CODE_2016 					VARCHAR					,
    			Tot_P_M 						INTEGER					,
    			Tot_P_F 						INTEGER					,
    			Tot_P_P 						INTEGER					,
    			Age_0_4_yr_M 					INTEGER					,
    			Age_0_4_yr_F 					INTEGER					,
    			Age_0_4_yr_P 					INTEGER					,
    			Age_5_14_yr_M 					INTEGER					,
    			Age_5_14_yr_F 					INTEGER					,
    			Age_5_14_yr_P 					INTEGER					,
    			Age_15_19_yr_M 					INTEGER					,
    			Age_15_19_yr_F 					INTEGER					,
    			Age_15_19_yr_P 					INTEGER					,
    			Age_20_24_yr_M 					INTEGER					,
    			Age_20_24_yr_F 					INTEGER					,
    			Age_20_24_yr_P 					INTEGER					,
    			Age_25_34_yr_M 					INTEGER					,
    			Age_25_34_yr_F 					INTEGER					,
    			Age_25_34_yr_P 					INTEGER					,
    			Age_35_44_yr_M 					INTEGER					,
    			Age_35_44_yr_F 					INTEGER					,
    			Age_35_44_yr_P 					INTEGER					,
    			Age_45_54_yr_M 					INTEGER					,
    			Age_45_54_yr_F 					INTEGER					,
    			Age_45_54_yr_P 					INTEGER					,
    			Age_55_64_yr_M 					INTEGER					,
    			Age_55_64_yr_F 					INTEGER					,
    			Age_55_64_yr_P 					INTEGER					,
    			Age_65_74_yr_M 					INTEGER					,
    			Age_65_74_yr_F 					INTEGER					,
    			Age_65_74_yr_P 					INTEGER					,
    			Age_75_84_yr_M 					INTEGER					,
    			Age_75_84_yr_F 					INTEGER					,
    			Age_75_84_yr_P 					INTEGER					,
    			Age_85ov_M 						INTEGER					,
    			Age_85ov_F 						INTEGER					,
    			Age_85ov_P 						INTEGER					,
    			Counted_Census_Night_home_M 	INTEGER					,
    			Counted_Census_Night_home_F 	INTEGER					,
    			Counted_Census_Night_home_P 	INTEGER					,
    			Count_Census_Nt_Ewhere_Aust_M 	INTEGER					,
    			Count_Census_Nt_Ewhere_Aust_F 	INTEGER					,
    			Count_Census_Nt_Ewhere_Aust_P 	INTEGER					,
    			Indigenous_psns_Aboriginal_M 	INTEGER					,
    			Indigenous_psns_Aboriginal_F 	INTEGER 				,
    			Indigenous_psns_Aboriginal_P 	INTEGER					,
    			Indig_psns_Torres_Strait_Is_M 	INTEGER					,
    			Indig_psns_Torres_Strait_Is_F 	INTEGER					,
    			Indig_psns_Torres_Strait_Is_P   INTEGER					,
    			Indig_Bth_Abor_Torres_St_Is_M   INTEGER					,
    			Indig_Bth_Abor_Torres_St_Is_F 	INTEGER					,
    			Indig_Bth_Abor_Torres_St_Is_P 	INTEGER					,
    			Indigenous_P_Tot_M 				INTEGER					,
    			Indigenous_P_Tot_F 				INTEGER					,
    			Indigenous_P_Tot_P 				INTEGER					,
    			Birthplace_Australia_M 			INTEGER					,
    			Birthplace_Australia_F 			INTEGER					,
    			Birthplace_Australia_P 			INTEGER					,
    			Birthplace_Elsewhere_M 			INTEGER					,
    			Birthplace_Elsewhere_F 			INTEGER					,
    			Birthplace_Elsewhere_P 			INTEGER					,
    			Lang_spoken_home_Eng_only_M 	INTEGER					,
    			Lang_spoken_home_Eng_only_F 	INTEGER					,
    			Lang_spoken_home_Eng_only_P 	INTEGER					,
    			Lang_spoken_home_Oth_Lang_M 	INTEGER					,
    			Lang_spoken_home_Oth_Lang_F 	INTEGER					,
    			Lang_spoken_home_Oth_Lang_P 	INTEGER					,
    			Australian_citizen_M 			INTEGER					,
    			Australian_citizen_F 			INTEGER					,
    			Australian_citizen_P 			INTEGER					,
    			Age_psns_att_educ_inst_0_4_M 	INTEGER					,
    			Age_psns_att_educ_inst_0_4_F 	INTEGER					,
    			Age_psns_att_educ_inst_0_4_P 	INTEGER					,
    			Age_psns_att_educ_inst_5_14_M   INTEGER					,
    			Age_psns_att_educ_inst_5_14_F 	INTEGER					,
    			Age_psns_att_educ_inst_5_14_P 	INTEGER					,
    			Age_psns_att_edu_inst_15_19_M 	INTEGER					,
    			Age_psns_att_edu_inst_15_19_F 	INTEGER					,
    			Age_psns_att_edu_inst_15_19_P 	INTEGER					,
    			Age_psns_att_edu_inst_20_24_M 	INTEGER					,
    			Age_psns_att_edu_inst_20_24_F 	INTEGER					,
    			Age_psns_att_edu_inst_20_24_P 	INTEGER					,
    			Age_psns_att_edu_inst_25_ov_M 	INTEGER					,
    			Age_psns_att_edu_inst_25_ov_F 	INTEGER					,
    			Age_psns_att_edu_inst_25_ov_P 	INTEGER					,
    			High_yr_schl_comp_Yr_12_eq_M 	INTEGER					,
    			High_yr_schl_comp_Yr_12_eq_F 	INTEGER					,
    			High_yr_schl_comp_Yr_12_eq_P 	INTEGER					,
    			High_yr_schl_comp_Yr_11_eq_M 	INTEGER					,
    			High_yr_schl_comp_Yr_11_eq_F 	INTEGER					,
    			High_yr_schl_comp_Yr_11_eq_P 	INTEGER					,
    			High_yr_schl_comp_Yr_10_eq_M 	INTEGER					,
    			High_yr_schl_comp_Yr_10_eq_F 	INTEGER					,
    			High_yr_schl_comp_Yr_10_eq_P 	INTEGER					,
    			High_yr_schl_comp_Yr_9_eq_M 	INTEGER					,
    			High_yr_schl_comp_Yr_9_eq_F 	INTEGER					,
    			High_yr_schl_comp_Yr_9_eq_P 	INTEGER					,
    			High_yr_schl_comp_Yr_8_belw_M 	INTEGER					,
    			High_yr_schl_comp_Yr_8_belw_F 	INTEGER					,
    			High_yr_schl_comp_Yr_8_belw_P 	INTEGER					,
    			High_yr_schl_comp_D_n_g_sch_M 	INTEGER					,
    			High_yr_schl_comp_D_n_g_sch_F 	INTEGER					,
    			High_yr_schl_comp_D_n_g_sch_P 	INTEGER					,
    			Count_psns_occ_priv_dwgs_M 		INTEGER					,
    			Count_psns_occ_priv_dwgs_F 		INTEGER					,
    			Count_psns_occ_priv_dwgs_P 		INTEGER					,
    			Count_Persons_other_dwgs_M 		INTEGER					,
    			Count_Persons_other_dwgs_F 		INTEGER					,
    			Count_Persons_other_dwgs_P 		INTEGER
);

-- 2016_Census_G01_NSW_LGA

create table if not exists
				raw.Table_G02_NSW_LGA
(

				LGA_CODE_2016 					VARCHAR					,
    			Median_age_persons 				INTEGER					,
    			Median_mortgage_repay_monthly 	INTEGER					,
    			Median_tot_prsnl_inc_weekly 	INTEGER					,
    			Median_rent_weekly 				INTEGER					,
    			Median_tot_fam_inc_weekly 		INTEGER					,
    			Average_num_psns_per_bedroom 	INTEGER					,
    			Median_tot_hhd_inc_weekly 		INTEGER					,
    			Average_household_size 			INTEGER
);

-- Listings

CREATE TABLE if not exists
				raw.Table_Listings (
				Listing_id 						BIGINT					,
    			Scrape_id 						BIGINT					,
    			Scraped_date 					DATE					,
    			Host_id 						BIGINT					,
    			Host_name 						VARCHAR					,
    			Host_since 						DATE					,
    			Host_is_superhost 				VARCHAR					,
    			Host_neighbourhood 				VARCHAR					,
    			Listing_neighbourhood 			VARCHAR					,
    			Property_type 					VARCHAR					,
    			Room_type 						VARCHAR					,
    			Accommodates 					INTEGER					,
    			Price 							numeric					,
    			Has_availability 				VARCHAR					,
    			Availability_30 				INTEGER					,
    			Number_of_reviews 				INTEGER					,
    			Review_scores_rating 			numeric					,
    			Review_scores_accuracy 			numeric					,
    			Review_scores_cleanliness 		numeric					,
    			Review_scores_checkin 			numeric					,
    			Review_scores_communication 	numeric					,
    			Review_scores_value 			NUMERIC
);

-- NSW_LGA_Code

create table 
if not exists
				raw.Table_NSW_LGA_Code
(
			 	Lga_code						INTEGER 	PRIMARY key	,
    			Lga_name						VARCHAR
);

-- NSW_LGA_Suburb

create table
	if not exists
				raw.Table_NSW_LGA_Suburb
(
				Lga_name 						VARCHAR					,
    			Suburb_name 					VARCHAR				
);

select * from raw.Table_G02_NSW_LGA;

select count(*) from raw.Table_NSW_LGA_Suburb;



/*
				
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'raw';



*/

select * from raw.Table_NSW_LGA_Code;
delete from raw.Table_NSW_LGA_Code;
select * from raw.Table_NSW_LGA_Code;
delete from raw.Table_NSW_LGA_Suburb;
select * from raw.Table_NSW_LGA_Suburb;
delete from raw.Table_Listings;
select * from raw.Table_Listings;
delete from raw.Table_G01_NSW_LGA;
select * from raw.Table_G01_NSW_LGA;
delete from raw.Table_G02_NSW_LGA;
select * from raw.Table_G02_NSW_LGA;


--------------------------------
-- Check the count of each table
--------------------------------

SELECT 'Table_G01_NSW_LGA' AS table_name, COUNT(*) FROM raw.Table_G01_NSW_LGA
UNION ALL
SELECT 'Table_G02_NSW_LGA', COUNT(*) FROM raw.Table_G02_NSW_LGA
UNION ALL
SELECT 'table_listings', COUNT(*) FROM raw.Table_Listings
UNION ALL
SELECT 'Table_NSW_LGA_Code', COUNT(*) FROM raw.Table_NSW_LGA_Code
UNION ALL
SELECT 'Table_NSW_LGA_Suburb', COUNT(*) FROM raw.Table_NSW_LGA_Suburb;

-------------------------------
------ OUTPUT -----------------
-------------------------------
/*
table_name          |count |
--------------------+------+
Table_G01_NSW_LGA   |   132|
Table_G02_NSW_LGA   |   132|
table_listings      |412122|
Table_NSW_LGA_Code  |   129|
Table_NSW_LGA_Suburb|  4470|
*/

--- End of Part_1 _ Question 2 ------
