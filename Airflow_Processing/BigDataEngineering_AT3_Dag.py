#### DAG file Creation -- Part 1 Question 3 ---

# Importing Neccessary Packages
import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Dag Settings 

dag_default_args = {
    'owner': 'Steffi-24592774',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='BigDataEngineering_AT3_24592774_Data_Ingestion',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

# Environment Variables

# Census Data 

Census_LGA_G01_Path="/home/airflow/gcs/data/Census LGA/2016Census_G01_NSW_LGA.csv"
Census_LGA_G02_Path="/home/airflow/gcs/data/Census LGA/2016Census_G02_NSW_LGA.csv"

# Listing Data

Listing_Path="/home/airflow/gcs/data/Listings"

# Nsw-lga Data
NSW_LGA_Code_Path="/home/airflow/gcs/data/NSW LGA/NSW_LGA_CODE.csv"
NSW_LGA_Suburb_Path="/home/airflow/gcs/data/NSW LGA/NSW_LGA_SUBURB.csv"

# Custom Logics for Operator

def Load_Table_NSW_LGA_Code_Function(**kwargs):

    # Connection String
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    NSW_LGA_Code = pd.read_csv(NSW_LGA_Code_Path)

    NSW_LGA_Code_Data=NSW_LGA_Code[['LGA_CODE','LGA_NAME']]

    if len(NSW_LGA_Code_Data) > 0:
        NSW_LGA_Code_Columns = ['LGA_CODE','LGA_NAME'] # Name as in .CSV

        values = NSW_LGA_Code_Data[NSW_LGA_Code_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                        INTO
                            raw.table_nsw_lga_code (
                                                    LGA_CODE    ,
                                                    LGA_NAME
                                                    )
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(NSW_LGA_Code_Data))
        conn_ps.commit()
    else:
        None

    return None


def Load_Table_NSW_LGA_Suburb_Function(**kwargs):

    # Connection String
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    NSW_LGA_Suburb = pd.read_csv(NSW_LGA_Suburb_Path)


    NSW_LGA_Suburb_Data=NSW_LGA_Suburb[['LGA_NAME','SUBURB_NAME']]

    if len(NSW_LGA_Suburb_Data) > 0:
        NSW_LGA_Suburb_Columns = ['LGA_NAME','SUBURB_NAME']

        values = NSW_LGA_Suburb_Data[NSW_LGA_Suburb_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                        INTO 
                            raw.table_nsw_lga_suburb(lga_name   ,
                                                    suburb_name
                                                    )
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(NSW_LGA_Suburb_Data))
        conn_ps.commit()
    else:
        None

    return None


def Load_Table_Listings_Data_2020_Function(**kwargs):

    # Connection String
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    Listing_2020_File = [f for f in os.listdir(Listing_Path) if f.endswith('2020.csv')]
    Listing_2020_Data = pd.DataFrame()

    
    for file in Listing_2020_File:
        file_path = os.path.join(Listing_Path, file)
        Listing_2020_Read = pd.read_csv(file_path)
    
        Listing_2020_Data = pd.concat([Listing_2020_Data, Listing_2020_Read], ignore_index=True)

    Listing_2020_Data['SCRAPED_DATE'] = pd.to_datetime(Listing_2020_Data['SCRAPED_DATE'], 
                                                       format='%Y-%m-%d').dt.strftime('%Y/%m/%d')
    Listing_2020_Data['HOST_SINCE'] = pd.to_datetime(Listing_2020_Data['HOST_SINCE'], 
                                                     format='%d/%m/%Y', errors='coerce').dt.strftime('%Y/%m/%d')
    

    Missing_Data = '2020/05/01' # Missing Date is found in May File so replacing with the first day of the month
    Listing_2020_Data['HOST_SINCE'] = Listing_2020_Data['HOST_SINCE'].fillna(Missing_Data)
    Listing_2020_Data['SCRAPED_DATE'] = Listing_2020_Data['SCRAPED_DATE'].fillna(Missing_Data)


    if len(Listing_2020_Data) > 0:
        Listing_2020_Columns = [
                                'LISTING_ID',
                                'SCRAPE_ID',
                                'SCRAPED_DATE',
                                'HOST_ID',
                                'HOST_NAME',
                                'HOST_SINCE',
                                'HOST_IS_SUPERHOST',
                                'HOST_NEIGHBOURHOOD',
                                'LISTING_NEIGHBOURHOOD',
                                'PROPERTY_TYPE',
                                'ROOM_TYPE',
                                'ACCOMMODATES',
                                'PRICE',
                                'HAS_AVAILABILITY',
                                'AVAILABILITY_30',
                                'NUMBER_OF_REVIEWS',
                                'REVIEW_SCORES_RATING',
                                'REVIEW_SCORES_ACCURACY',
                                'REVIEW_SCORES_CLEANLINESS',
                                'REVIEW_SCORES_CHECKIN',
                                'REVIEW_SCORES_COMMUNICATION',
                                'REVIEW_SCORES_VALUE']


        values = Listing_2020_Data[Listing_2020_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                            INTO 
                            raw.Table_Listings(
                                                Listing_id, 
                                                Scrape_id, 
                                                Scraped_Date, 
                                                Host_id, 
                                                Host_name, 
                                                Host_since, 
                                                Host_is_superhost, 
                                                Host_neighbourhood, 
                                                Listing_neighbourhood, 
                                                Property_type, 
                                                Room_type, 
                                                Accommodates, 
                                                Price, 
                                                Has_availability,
                                                Availability_30, 
                                                Number_of_reviews, 
                                                Review_scores_rating, 
                                                Review_scores_accuracy, 
                                                Review_scores_cleanliness, 
                                                Review_scores_checkin, 
                                                Review_scores_communication, 
                                                Review_scores_value)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(Listing_2020_Data))
        conn_ps.commit()
    else:
        None

    return None


def Load_Table_Listings_Data_2021_Function(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    Listing_2021_File = [f for f in os.listdir(Listing_Path) if f.endswith('2021.csv')]
    Listing_2021_Data = pd.DataFrame()

    # Loop through the CSV files and merge them into the DataFrame
    for file in Listing_2021_File:
        file_path = os.path.join(Listing_Path, file)
        Listing_2021_Read = pd.read_csv(file_path)
    
        Listing_2021_Data = pd.concat([Listing_2021_Data, Listing_2021_Read], ignore_index=True)

    Listing_2021_Data['SCRAPED_DATE'] = pd.to_datetime(Listing_2021_Data['SCRAPED_DATE'], 
                                                       format='%Y-%m-%d').dt.strftime('%Y/%m/%d')
    Listing_2021_Data['HOST_SINCE'] = pd.to_datetime(Listing_2021_Data['HOST_SINCE'], 
                                                     format='%d/%m/%Y', errors='coerce').dt.strftime('%Y/%m/%d')
    

    Missing_Date = '2021/01/01'  # Missing Date is found in January File so replacing with the first date
    Listing_2021_Data['HOST_SINCE'] = Listing_2021_Data['HOST_SINCE'].fillna(Missing_Date)
    Listing_2021_Data['SCRAPED_DATE'] = Listing_2021_Data['SCRAPED_DATE'].fillna(Missing_Date)


    if len(Listing_2021_Data) > 0:
        Listing_2021_Columns = ['LISTING_ID','SCRAPE_ID','SCRAPED_DATE','HOST_ID','HOST_NAME','HOST_SINCE','HOST_IS_SUPERHOST','HOST_NEIGHBOURHOOD','LISTING_NEIGHBOURHOOD','PROPERTY_TYPE','ROOM_TYPE','ACCOMMODATES','PRICE','HAS_AVAILABILITY','AVAILABILITY_30','NUMBER_OF_REVIEWS','REVIEW_SCORES_RATING','REVIEW_SCORES_ACCURACY','REVIEW_SCORES_CLEANLINESS','REVIEW_SCORES_CHECKIN','REVIEW_SCORES_COMMUNICATION','REVIEW_SCORES_VALUE']

        values = Listing_2021_Data[Listing_2021_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                        INTO 
                        raw.table_listings(
                                            Listing_id, 
                                            Scrape_id, 
                                            Scraped_Date, 
                                            Host_id, 
                                            Host_name, 
                                            Host_since, 
                                            Host_is_superhost, 
                                            Host_neighbourhood, 
                                            Listing_neighbourhood, 
                                            Property_type, 
                                            Room_type, 
                                            Accommodates, 
                                            Price, 
                                            Has_availability,
                                            Availability_30, 
                                            Number_of_reviews, 
                                            Review_scores_rating, 
                                            Review_scores_accuracy, 
                                            Review_scores_cleanliness, 
                                            Review_scores_checkin, 
                                            Review_scores_communication, 
                                            Review_scores_value
                                            )
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(Listing_2021_Data))
        conn_ps.commit()
    else:
        None

    return None


def Load_Table_G01_NSW_LGA_Function(**kwargs):

    # Connection String
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    Census_LGA_G01_Data = pd.read_csv(Census_LGA_G01_Path)

    if len(Census_LGA_G01_Data) > 0:
        Census_LGA_G01_Columns = ['LGA_CODE_2016', 
                                  'Tot_P_M', 
                                  'Tot_P_F', 
                                  'Tot_P_P', 
                                  'Age_0_4_yr_M', 
                                  'Age_0_4_yr_F', 
                                  'Age_0_4_yr_P', 
                                  'Age_5_14_yr_M', 
                                  'Age_5_14_yr_F', 
                                  'Age_5_14_yr_P', 
                                  'Age_15_19_yr_M', 
                                  'Age_15_19_yr_F', 
                                  'Age_15_19_yr_P', 
                                  'Age_20_24_yr_M', 
                                  'Age_20_24_yr_F', 
                                  'Age_20_24_yr_P', 
                                  'Age_25_34_yr_M', 
                                  'Age_25_34_yr_F', 
                                  'Age_25_34_yr_P', 
                                  'Age_35_44_yr_M', 
                                  'Age_35_44_yr_F', 
                                  'Age_35_44_yr_P', 
                                  'Age_45_54_yr_M', 
                                  'Age_45_54_yr_F', 
                                  'Age_45_54_yr_P', 
                                  'Age_55_64_yr_M', 
                                  'Age_55_64_yr_F', 
                                  'Age_55_64_yr_P', 
                                  'Age_65_74_yr_M', 
                                  'Age_65_74_yr_F', 
                                  'Age_65_74_yr_P', 
                                  'Age_75_84_yr_M', 
                                  'Age_75_84_yr_F', 
                                  'Age_75_84_yr_P', 
                                  'Age_85ov_M', 
                                  'Age_85ov_F', 
                                  'Age_85ov_P', 
                                  'Counted_Census_Night_home_M', 
                                  'Counted_Census_Night_home_F', 
                                  'Counted_Census_Night_home_P', 
                                  'Count_Census_Nt_Ewhere_Aust_M', 
                                  'Count_Census_Nt_Ewhere_Aust_F', 
                                  'Count_Census_Nt_Ewhere_Aust_P', 
                                  'Indigenous_psns_Aboriginal_M', 
                                  'Indigenous_psns_Aboriginal_F', 
                                  'Indigenous_psns_Aboriginal_P', 
                                  'Indig_psns_Torres_Strait_Is_M', 
                                  'Indig_psns_Torres_Strait_Is_F', 
                                  'Indig_psns_Torres_Strait_Is_P', 
                                  'Indig_Bth_Abor_Torres_St_Is_M', 
                                  'Indig_Bth_Abor_Torres_St_Is_F', 
                                  'Indig_Bth_Abor_Torres_St_Is_P', 
                                  'Indigenous_P_Tot_M', 
                                  'Indigenous_P_Tot_F', 
                                  'Indigenous_P_Tot_P', 
                                  'Birthplace_Australia_M', 
                                  'Birthplace_Australia_F', 
                                  'Birthplace_Australia_P', 
                                  'Birthplace_Elsewhere_M', 
                                  'Birthplace_Elsewhere_F', 
                                  'Birthplace_Elsewhere_P', 
                                  'Lang_spoken_home_Eng_only_M', 
                                  'Lang_spoken_home_Eng_only_F', 
                                  'Lang_spoken_home_Eng_only_P', 
                                  'Lang_spoken_home_Oth_Lang_M', 
                                  'Lang_spoken_home_Oth_Lang_F', 
                                  'Lang_spoken_home_Oth_Lang_P', 
                                  'Australian_citizen_M', 
                                  'Australian_citizen_F', 
                                  'Australian_citizen_P', 
                                  'Age_psns_att_educ_inst_0_4_M', 
                                  'Age_psns_att_educ_inst_0_4_F', 
                                  'Age_psns_att_educ_inst_0_4_P', 
                                  'Age_psns_att_educ_inst_5_14_M', 
                                  'Age_psns_att_educ_inst_5_14_F', 
                                  'Age_psns_att_educ_inst_5_14_P', 
                                  'Age_psns_att_edu_inst_15_19_M', 
                                  'Age_psns_att_edu_inst_15_19_F', 
                                  'Age_psns_att_edu_inst_15_19_P', 
                                  'Age_psns_att_edu_inst_20_24_M', 
                                  'Age_psns_att_edu_inst_20_24_F', 
                                  'Age_psns_att_edu_inst_20_24_P', 
                                  'Age_psns_att_edu_inst_25_ov_M', 
                                  'Age_psns_att_edu_inst_25_ov_F', 
                                  'Age_psns_att_edu_inst_25_ov_P', 
                                  'High_yr_schl_comp_Yr_12_eq_M', 
                                  'High_yr_schl_comp_Yr_12_eq_F', 
                                  'High_yr_schl_comp_Yr_12_eq_P', 
                                  'High_yr_schl_comp_Yr_11_eq_M', 
                                  'High_yr_schl_comp_Yr_11_eq_F', 
                                  'High_yr_schl_comp_Yr_11_eq_P', 
                                  'High_yr_schl_comp_Yr_10_eq_M', 
                                  'High_yr_schl_comp_Yr_10_eq_F', 
                                  'High_yr_schl_comp_Yr_10_eq_P', 
                                  'High_yr_schl_comp_Yr_9_eq_M', 
                                  'High_yr_schl_comp_Yr_9_eq_F', 
                                  'High_yr_schl_comp_Yr_9_eq_P', 
                                  'High_yr_schl_comp_Yr_8_belw_M', 
                                  'High_yr_schl_comp_Yr_8_belw_F', 
                                  'High_yr_schl_comp_Yr_8_belw_P', 
                                  'High_yr_schl_comp_D_n_g_sch_M', 
                                  'High_yr_schl_comp_D_n_g_sch_F', 
                                  'High_yr_schl_comp_D_n_g_sch_P', 
                                  'Count_psns_occ_priv_dwgs_M', 
                                  'Count_psns_occ_priv_dwgs_F', 
                                  'Count_psns_occ_priv_dwgs_P', 
                                  'Count_Persons_other_dwgs_M', 
                                  'Count_Persons_other_dwgs_F', 
                                  'Count_Persons_other_dwgs_P']


        values = Census_LGA_G01_Data[Census_LGA_G01_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                        INTO 
                            raw.table_g01_nsw_lga
                                                (LGA_CODE_2016,
                                                Tot_P_M,
                                                Tot_P_F,
                                                Tot_P_P,
                                                Age_0_4_yr_M,
                                                Age_0_4_yr_F,
                                                Age_0_4_yr_P,
                                                Age_5_14_yr_M,
                                                Age_5_14_yr_F,
                                                Age_5_14_yr_P,
                                                Age_15_19_yr_M,
                                                Age_15_19_yr_F,
                                                Age_15_19_yr_P,
                                                Age_20_24_yr_M,
                                                Age_20_24_yr_F,
                                                Age_20_24_yr_P,
                                                Age_25_34_yr_M,
                                                Age_25_34_yr_F,
                                                Age_25_34_yr_P,
                                                Age_35_44_yr_M,
                                                Age_35_44_yr_F,
                                                Age_35_44_yr_P,
                                                Age_45_54_yr_M,
                                                Age_45_54_yr_F,
                                                Age_45_54_yr_P,
                                                Age_55_64_yr_M,
                                                Age_55_64_yr_F,
                                                Age_55_64_yr_P,
                                                Age_65_74_yr_M,
                                                Age_65_74_yr_F,
                                                Age_65_74_yr_P,
                                                Age_75_84_yr_M,
                                                Age_75_84_yr_F,
                                                Age_75_84_yr_P,
                                                Age_85ov_M,
                                                Age_85ov_F,
                                                Age_85ov_P,
                                                Counted_Census_Night_home_M,
                                                Counted_Census_Night_home_F,
                                                Counted_Census_Night_home_P,
                                                Count_Census_Nt_Ewhere_Aust_M,
                                                Count_Census_Nt_Ewhere_Aust_F,
                                                Count_Census_Nt_Ewhere_Aust_P,
                                                Indigenous_psns_Aboriginal_M,
                                                Indigenous_psns_Aboriginal_F,
                                                Indigenous_psns_Aboriginal_P,
                                                Indig_psns_Torres_Strait_Is_M,
                                                Indig_psns_Torres_Strait_Is_F,
                                                Indig_psns_Torres_Strait_Is_P,
                                                Indig_Bth_Abor_Torres_St_Is_M,
                                                Indig_Bth_Abor_Torres_St_Is_F,
                                                Indig_Bth_Abor_Torres_St_Is_P,
                                                Indigenous_P_Tot_M,
                                                Indigenous_P_Tot_F,
                                                Indigenous_P_Tot_P,
                                                Birthplace_Australia_M,
                                                Birthplace_Australia_F,
                                                Birthplace_Australia_P,
                                                Birthplace_Elsewhere_M,
                                                Birthplace_Elsewhere_F,
                                                Birthplace_Elsewhere_P,
                                                Lang_spoken_home_Eng_only_M,
                                                Lang_spoken_home_Eng_only_F,
                                                Lang_spoken_home_Eng_only_P,
                                                Lang_spoken_home_Oth_Lang_M,
                                                Lang_spoken_home_Oth_Lang_F,
                                                Lang_spoken_home_Oth_Lang_P,
                                                Australian_citizen_M,
                                                Australian_citizen_F,
                                                Australian_citizen_P,
                                                Age_psns_att_educ_inst_0_4_M,
                                                Age_psns_att_educ_inst_0_4_F,
                                                Age_psns_att_educ_inst_0_4_P,
                                                Age_psns_att_educ_inst_5_14_M,
                                                Age_psns_att_educ_inst_5_14_F,
                                                Age_psns_att_educ_inst_5_14_P,
                                                Age_psns_att_edu_inst_15_19_M,
                                                Age_psns_att_edu_inst_15_19_F,
                                                Age_psns_att_edu_inst_15_19_P,
                                                Age_psns_att_edu_inst_20_24_M,
                                                Age_psns_att_edu_inst_20_24_F,
                                                Age_psns_att_edu_inst_20_24_P,
                                                Age_psns_att_edu_inst_25_ov_M,
                                                Age_psns_att_edu_inst_25_ov_F,
                                                Age_psns_att_edu_inst_25_ov_P,
                                                High_yr_schl_comp_Yr_12_eq_M,
                                                High_yr_schl_comp_Yr_12_eq_F,
                                                High_yr_schl_comp_Yr_12_eq_P,
                                                High_yr_schl_comp_Yr_11_eq_M,
                                                High_yr_schl_comp_Yr_11_eq_F,
                                                High_yr_schl_comp_Yr_11_eq_P,
                                                High_yr_schl_comp_Yr_10_eq_M,
                                                High_yr_schl_comp_Yr_10_eq_F,
                                                High_yr_schl_comp_Yr_10_eq_P,
                                                High_yr_schl_comp_Yr_9_eq_M,
                                                High_yr_schl_comp_Yr_9_eq_F,
                                                High_yr_schl_comp_Yr_9_eq_P,
                                                High_yr_schl_comp_Yr_8_belw_M,
                                                High_yr_schl_comp_Yr_8_belw_F,
                                                High_yr_schl_comp_Yr_8_belw_P,
                                                High_yr_schl_comp_D_n_g_sch_M,
                                                High_yr_schl_comp_D_n_g_sch_F,
                                                High_yr_schl_comp_D_n_g_sch_P,
                                                Count_psns_occ_priv_dwgs_M,
                                                Count_psns_occ_priv_dwgs_F,
                                                Count_psns_occ_priv_dwgs_P,
                                                Count_Persons_other_dwgs_M,
                                                Count_Persons_other_dwgs_F,
                                                Count_Persons_other_dwgs_P)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(Census_LGA_G01_Data))
        conn_ps.commit()
    else:
        None

    return None


def Load_Table_G02_NSW_LGA_Function(**kwargs):

    # Connection String
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    Census_LGA_G02_Data= pd.read_csv(Census_LGA_G02_Path)


    if len(Census_LGA_G02_Data) > 0:
        Census_LGA_G02_Columns = ['LGA_CODE_2016',
                                  'Median_age_persons',
                                  'Median_mortgage_repay_monthly',
                                  'Median_tot_prsnl_inc_weekly',
                                  'Median_rent_weekly',
                                  'Median_tot_fam_inc_weekly',
                                  'Average_num_psns_per_bedroom'
                                  ,'Median_tot_hhd_inc_weekly',
                                  'Average_household_size'
                                  ]

        values = Census_LGA_G02_Data[Census_LGA_G02_Columns].to_dict('split')
        values = values['data']
        logging.info(values)

        Insert_Query = """
                    INSERT 
                        INTO 
                            raw.table_g02_nsw_lga
                                    (
                                    LGA_CODE_2016,
                                    Median_age_persons,
                                    Median_mortgage_repay_monthly,
                                    Median_tot_prsnl_inc_weekly,
                                    Median_rent_weekly,
                                    Median_tot_fam_inc_weekly,
                                    Average_num_psns_per_bedroom,
                                    Median_tot_hhd_inc_weekly,
                                    Average_household_size
                                    )
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), Insert_Query, values, page_size=len(Census_LGA_G02_Data))
        conn_ps.commit()
    else:
        None

    return None


# Setting DAG Operator

Load_NSW_LGA_code_task = PythonOperator(
    task_id="Load_NSW_LGA_Code_Task",
    python_callable=Load_Table_NSW_LGA_Code_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

Load_NSW_LGA_suburb_task = PythonOperator(
    task_id="Load_NSW_LGA_Suburb_Task",
    python_callable=Load_Table_NSW_LGA_Suburb_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

Load_LISTINGS_2020_task = PythonOperator(
    task_id="Load_Listings_Data_2020_Task",
    python_callable=Load_Table_Listings_Data_2020_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

Load_LISTINGS_2021_task = PythonOperator(
    task_id="Load_Listings_Data_2021_Task",
    python_callable=Load_Table_Listings_Data_2021_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

Load_CENSUS_G01_LGA_task = PythonOperator(
    task_id="Load_G01_NSW_LGA_Task",
    python_callable=Load_Table_G01_NSW_LGA_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
Load_CENSUS_G02_LGA_task = PythonOperator(
    task_id="Load_G02_NSW_LGA_Task",
    python_callable=Load_Table_G02_NSW_LGA_Function,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
Load_LISTINGS_2020_task >> Load_LISTINGS_2021_task




