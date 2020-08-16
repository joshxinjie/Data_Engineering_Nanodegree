import os
from datetime import datetime, timedelta

import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook

from operators import (CreateS3BucketOperator, UploadFilesToS3Operator)

config = configparser.ConfigParser()
config.read('config/config.cfg')

AWS_REGION=config["AWS_CRED"]["REGION"]

LOCAL_RAW_DATA_DIR=config["LOCAL_RAW_DATA_DIR"]["RAW_DATA_DIR"]
LOCAL_RAW_IMM_DATA_DIR=config["LOCAL_RAW_DATA_DIR"]["IMM_DATA_DIR"]

S3_RAW_DATA_BUCKET=config["S3_RAW_DATA_BUCKET"]["BUCKET_NAME"]
S3_RAW_IMM_DATA_DIR=config["S3_RAW_DATA_BUCKET"]["IMM_DATA_DIR"] 

default_args = {
    'owner': 'xinjie',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'end_date': datetime(2020, 9, 1),
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # do not backfill dag runs
    'catchup': False
}

dag = DAG('upload_raw_data_to_s3_dag',
          default_args=default_args,
          description='Upload raw data from local directory to s3',
          schedule_interval='@monthly',
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_data_bucket = CreateS3BucketOperator(
    task_id='Create_raw_data_bucket_in_s3',
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_RAW_DATA_BUCKET,
    region=AWS_REGION,
    dag=dag
)

upload_raw_immigration_data_to_s3 = UploadFilesToS3Operator(
    task_id='Upload_raw_immigration_data_to_s3',
    aws_credentials_id="aws_credentials",
    source_folder=LOCAL_RAW_IMM_DATA_DIR,
    s3_target_folder=S3_RAW_IMM_DATA_DIR,
    s3_bucket=S3_RAW_DATA_BUCKET,
    file_extension="*.sas7bdat",
    dag=dag
)

upload_raw_immigration_data_dictionary_to_s3 = UploadFilesToS3Operator(
    task_id='Upload_raw_immigration_data_dictionary_to_s3',
    aws_credentials_id="aws_credentials",
    source_folder=LOCAL_RAW_DATA_DIR,
    s3_target_folder=None,
    s3_bucket=S3_RAW_DATA_BUCKET,
    file_extension="*.SAS",
    dag=dag
)

upload_raw_us_cities_demographics_data_to_s3 = UploadFilesToS3Operator(
    task_id='Upload_raw_us_cities_demog_data_to_s3',
    aws_credentials_id="aws_credentials",
    source_folder=LOCAL_RAW_DATA_DIR,
    s3_target_folder=None,
    s3_bucket=S3_RAW_DATA_BUCKET,
    file_extension="*.csv",
    dag=dag
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

start_operator >> create_raw_data_bucket

create_raw_data_bucket >> upload_raw_immigration_data_to_s3
create_raw_data_bucket >> upload_raw_immigration_data_dictionary_to_s3
create_raw_data_bucket >> upload_raw_us_cities_demographics_data_to_s3

upload_raw_immigration_data_to_s3 >> end_operator
upload_raw_immigration_data_dictionary_to_s3 >> end_operator
upload_raw_us_cities_demographics_data_to_s3 >> end_operator