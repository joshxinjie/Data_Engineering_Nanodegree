import os
from datetime import datetime, timedelta

import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.base_hook import BaseHook

from operators import (CreateS3BucketOperator, UploadFilesToS3Operator)

config = configparser.ConfigParser()
config.read('config/config.cfg')

# need to copy config folder in docker compose file. Note that airflow dag and plugins are copied to a different path

EMR_INSTANCE_TYPE=config["EMR"]["INSTANCE_TYPE"]
NUM_MASTER_NODES=int(config["EMR"]["NUM_MASTER_NODES"])
NUM_CORE_NODES=int(config["EMR"]["NUM_CORE_NODES"])
S3_CODE_BUCKET_NAME=config["EMR"]["S3_CODE_BUCKET_NAME"]
S3_CODE_KEY_NAME=config["EMR"]["S3_CODE_KEY_NAME"]
S3_LOGS_BUCKET_NAME=config["EMR"]["S3_LOGS_BUCKET_NAME"]
AWS_REGION=config["AWS_CRED"]["REGION"]
EMR_ETL_SCRIPTS_DIR=config["EMR_ETL_SCRIPTS"]["SCRIPTS_DIR"]


connection = BaseHook.get_connection("aws_credentials")
AWS_ACCESS_KEY_ID = connection.login
AWS_SECRET_ACCESS_KEY = connection.password

# EMR_STEPS = [
#     {
#         'Name': 'Setup Debugging',
#         'ActionOnFailure': 'TERMINATE_CLUSTER',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['state-pusher-script']
#         }
#     },
#     {
#         'Name': 'setup - copy files',
#         'ActionOnFailure': 'CANCEL_AND_WAIT',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['aws', 's3', 'cp', 's3://' + S3_CODE_BUCKET_NAME, '/home/hadoop/', '--recursive']
#         }
#     },
#     {
#         'Name': 'setup - install libraries',
#         'ActionOnFailure': 'CANCEL_AND_WAIT',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['sudo','pip','install','configparser']
#         }
#     },
#     {
#         'Name': 'Run ETL on Spark',
#         'ActionOnFailure': 'CANCEL_AND_WAIT',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['spark-submit', '/home/hadoop/src/spark_etl.py', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]
#         }
#     }
# ]

# JOB_FLOW_OVERRIDES = {
#     'Name': 'spark-emr-cluster',
#     'ReleaseLabel': 'emr-5.29.0',
#     'LogUri': os.path.join('s3://', S3_LOGS_BUCKET_NAME, 'elasticmapreduce/'),
#     'Instances': {
#         'InstanceGroups': [
#             {
#                 'Name': 'Master node',
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'MASTER',
#                 'InstanceType': EMR_INSTANCE_TYPE,
#                 'InstanceCount': NUM_MASTER_NODES,
#             },
#             {
#                 'Name': "Core nodes",
#                 'Market': 'ON_DEMAND',
#                 'InstanceRole': 'CORE',
#                 'InstanceType': EMR_INSTANCE_TYPE,
#                 'InstanceCount': NUM_CORE_NODES,
#             }
#         ],
#         'KeepJobFlowAliveWhenNoSteps': False,
#         'TerminationProtected': False,
#     },
#     'Steps': EMR_STEPS,
#     'JobFlowRole': 'EMR_EC2_DefaultRole',
#     'ServiceRole': 'EMR_DefaultRole',
# }

default_args = {
    'owner': 'xinjie',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'end_date': datetime(2020, 9, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # do not backfill dag runs
    'catchup': False
}

dag = DAG('immigration_etl_dag',
          default_args=default_args,
          description='Load and transform data in s3 with AWS EMR',
          schedule_interval='@monthly',
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    aws_credentials_id="aws_credentials",
    s3_bucket=S3_CODE_BUCKET_NAME,
    region=AWS_REGION,
    dag=dag
)

upload_files_to_s3 = UploadFilesToS3Operator(
    task_id='Upload_files_to_S3',
    aws_credentials_id="aws_credentials",
    source_folder=EMR_ETL_SCRIPTS_DIR,
    s3_target_folder=S3_CODE_KEY_NAME,
    s3_bucket=S3_CODE_BUCKET_NAME,
    file_extension="*.py",
    dag=dag
)

# cluster_creator = EmrCreateJobFlowOperator(
#         task_id='create_job_flow',
#         job_flow_overrides=JOB_FLOW_OVERRIDES,
#         aws_conn_id='aws_default',
#         emr_conn_id='emr_default'
#     )

# step_adder = EmrAddStepsOperator(
#     task_id='add_steps',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
#     aws_conn_id='aws_default',
#     steps=EMR_STEPS
# )

# step_checker = EmrStepSensor(
#     task_id='watch_step',
#     job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
#     step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
#     aws_conn_id='aws_default'
# )

# cluster_remover = EmrTerminateJobFlowOperator(
#     task_id='remove_cluster',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
#     aws_conn_id='aws_default'
# )


start_operator >> create_code_bucket
create_code_bucket >> upload_files_to_s3