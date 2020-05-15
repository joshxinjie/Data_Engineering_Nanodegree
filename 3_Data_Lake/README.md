# Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task for this project will be to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We will use AWS Elastic Map Reduce (EMR) as well as S3 to manage our Data Lake and run our ETL pipeline.

## Database Schema

The Star schema is used for the analytics tables on the AWS EMR cluster, since the schema optimizes queries on song play analysis. There are other benefits for using the Star schema. These includes having denormalize tables, simplified queries, and fast aggregation of the data. The Star schema is usually less ideal for handling one-to-many or many-to-many relationships between the tables. Hence, the following tables are created to have only one-to-one relationships. Below is a list of the analytics tables.


**Fact Table**

**songplay** - records in log data associated with song plays i.e. records with page NextSong
* songplay_id (int) Primary Key : ID for each songplay
* start_time (timestamp) : Start time of songplay session
* user_id (int) : User's ID
* level (varchar) : User's membership level {free | paid}
* song_id (varchar) : Song's ID
* artist_id (varchar) : Artist's ID
* session_id (int) : ID of user's current session
* location (varchar) : User's location
* user_agent (varchar) : User's software agent

**Dimension Tables**

**users** - users in the app
* user_id (int) : ID of user
* first_name (varchar) : User's first name
* last_name (varchar) : User's last name
* gender (varchar) : User's gender
* level (varchar) : User membership level {free | paid}

**songs** - songs in music database
* song_id (varchar) : Song's ID
* title (varchar) : Song's title
* artist_id (varchar) : Artist's ID
* year (int) : Year of song release
* duration (float) : Duration of song

**artists** - artists in music database
* artist_id (varchar) : Artist's ID
* name (varchar) : Artist's name
* location (varchar) : Artist's location
* latitude (float) : Latitude of artist's location
* longitude (float) : Longitude of artist's location

**time** - timestamps of records in songplays broken down into specific units
* start_time (timestamp) : Starting timestamp for songplay
* hour (int) : The hour of the songplay's start
* day (int) : The day of the songplay's start
* week (int) : The week of the songplay's start
* month (int) : The month of the songplay's start
* year (int) : The year of the songplay's start
* weekday (int) : The day of the week of the songplay's start

## JSON Data in S3

There are two directories of JSON files. 

**song_data**: JSON files in this directory contain song metadata. This dataset is a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

An example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

**log_data**: JSON files in this directory contain logs on user activity. These log datasets are generated using by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above, and they will simulate app activity logs from an imaginary music streaming app based on configuration settings. The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

An example of of what the data in a log file, 2018-11-12-events.json, looks like

<img src="images/log-data.png" alt="drawing" width="800"/>

**log_json_path.json**: A JSONPath file mapping the JSON elements in the log_data to the appropriate columns in the Redshuft staging table `staging_events` .

## ETL Pipeline

The ETL pipeline will first create the AWS EMR clusters as well as the S3 buckets for storing the analytics tables and the EMR clusters' log files using the AWS python SDK boto3. Next, it will load the JSON files located in both the log_data and song_data directory on S3 into the EMR clusters. The EMR clusters will run the ETL script to generate the analytics tables, and then write them to partitioned parquet files in separate analytics directory in S3, and terminate the clusters. Each table has its own folder within the directory. The Songs table files are partitioned by year and then artist. The Time table files are partitioned by year and month. Finally, the Songplays table files are partitioned by year and month.

## Repository Structure

1. dl.cfg : The configuration file
2. etl.py : Python script to run the ETL pipeline that will load the data from S3, process the data using Spark, load the results back into S3.
3. run_emr_job.py : Python script to create the AWS EMR clusters as well as the S3 buckets for storing the dimensional tables output and the EMR clusters' log files using the AWS python SDK boto3
4. README.md : A report outlining the summary of the project, instructions on running the Python scripts, and an explanation of the files in the repository. It also provides details on the database schema design as well as the ETL pipeline
5. images : Folder containing images used in this readme

## How to Run

1. Fill in the settings under the AWS and DWH tags in the configuration file `dl.cfg` .
```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

[S3]
INPUT_BUCKET=s3a://udacity-dend/
CODE_BUCKET_NAME=
OUTPUT_BUCKET_NAME=
EMR_LOG_BUCKET_NAME=
```

2. In the main directory, run `run_emr_job.py`. The script will create the EMR and S3 resources, run the ETL pipeline, and then terminate the EMR resources once the ETL job is complete.

```
python -m run_emr_job
```