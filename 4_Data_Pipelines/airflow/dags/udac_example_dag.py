from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    #'end_date': datetime(2019, 1, 13),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # do not backfill dag runs
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=timedelta(hours=1),
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id="Create_staging_events_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id="Create_staging_songs_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id="Create_songplays_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplays_table_create
)

create_songs_table = PostgresOperator(
    task_id="Create_songs_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_create
)

create_users_table = PostgresOperator(
    task_id="Create_users_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.users_table_create
)

create_artists_table = PostgresOperator(
    task_id="Create_artists_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artists_table_create
)

create_time_table = PostgresOperator(
    task_id="Create_time_table",
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="'us-west-2'",
    json_option="'s3://udacity-dend/log_json_path.json'",
    time_format="'epochmillisecs'",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    #s3_key="song_data/A/A/A",
    s3_key="song_data",
    region="'us-west-2'",
    json_option="'auto'",
    time_format="'epochmillisecs'",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_sql_query=SqlQueries.songplay_table_insert,
    truncate=False
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_sql_query=SqlQueries.user_table_insert,
    truncate=True
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_sql_query=SqlQueries.song_table_insert,
    truncate=True
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_sql_query=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_sql_query=SqlQueries.time_table_insert,
    truncate=True
)

run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    check_null=False
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table
start_operator >> create_songplays_table
start_operator >> create_songs_table
start_operator >> create_users_table
start_operator >> create_artists_table
start_operator >> create_time_table

create_staging_events_table >> schema_created
create_staging_songs_table >> schema_created
create_songplays_table >> schema_created
create_songs_table >> schema_created
create_users_table >> schema_created
create_artists_table >> schema_created
create_time_table >> schema_created

schema_created >> stage_events_to_redshift
schema_created >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table

load_songs_dimension_table >> run_data_quality_checks
load_users_dimension_table >> run_data_quality_checks
load_artists_dimension_table >> run_data_quality_checks
load_time_dimension_table >> run_data_quality_checks

run_data_quality_checks >> end_operator