# Data Warehouse with AWS Redshift


## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task for this project will be to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

The project will automate the creation of the AWS Redshift clusters using the AWS SDK boto3. The technique to create infrastructure (e.g. machines, users, roles, folders and processes) with code is known as Infrastructure-as-Code (IaC).

AWS Redshift is a cloud-managed Data Warehouse storage based on a modified version of postgresql. Redshift is a column-oriented storage and it is able to process the database on a column by column basis and perform aggregation operation on them, without the need to process the entire row. Redshift is also a Massively Parallel Processing (MPP) database, which allows for parallelization of a single query execution on multiple CPUs/machines.


## Database Schema

The Star schema is used for the analytics tables on the AWS Redshift cluster, since the schema optimizes queries on song play analysis. There are other benefits for using the Star schema. These includes having denormalize tables, simplified queries, and fast aggregation of the data. The Star schema is usually less ideal for handling one-to-many or many-to-many relationships between the tables. Hence, the following tables are created to have only one-to-one relationships. Below is a list of the analytics tables.


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


Below is a list of the two staging tables created on the Redshift cluster.

**staging_events** - staging table for the JSON files in the log_data directory on S3
* artist (varchar)
* auth (varchar)
* firstNAME (varchar)
* gender (varchar)
* itemInSession (int)
* lastName (varchar)
* length (float)
* level (varchar)
* location (varchar)
* method (varchar)
* page (varchar)
* registration (float)
* sessionId (int)
* song (varchar)
* status (int)
* ts (timestamp)
* userAgent (varchar)
* userId (int)

**staging_songs** - staging table for the JSON files in the song_data directory on S3
* num_songs (int)
* artist_id (varchar)
* artist_latitude (float)
* artist_longitude (float)
* artist_location (varchar)
* artist_name (varchar)
* song_id (varchar)
* title (varchar)
* duration (float)
* year (int)


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

<img src="images/log-data.png" alt="drawing" width="600"/>

**log_json_path.json**: A JSONPath file mapping the JSON elements in the log_data to the appropriate columns in the Redshuft staging table `staging_events` .


## ETL Pipeline

The ETL pipeline will load the JSON files in the log_data directory on S3 into a staging table `staging_events` using a JSONPath file `log_json_path.json` to map the JSON elements to the columns in the `staging_events` table.

Next, the ETL pipeline will load the JSON files in the song_data directory on S3 into s a staging table `staging_songs`.

Finally, the ETL pipeline will load the data from the staging tables and insert the appropriate data into the analytics tables: `songplay`, `users`, `songs`, `artists` and `time` .


## Repository Structure

1. create_cluster.py : Python script to create the Redshift clusters using the AWS python SDK boto3
2. create_tables.py : Python script to create the fact and dimension analytics tables on Redshift
3. delete_cluster.py : Python script to delete the Redshift clusters using the AWS python SDK
4. etl.py : Python script to run the ETL pipeline that will load the data from S3 into staging tables on Redshift. It will then load the data from the staging tables to the analytics tables on Redshift
5. example_queries.py : Script containing basic examples of SQL queries and results for song play analysis
6. sql_queries.py : Script containing the SQL statements
7. dwh.cfg : The configuration file
8. README.md : A report outlining the summary of the project, instructions on running the Python scripts, and an explanation of the files in the repository. It also provides details on the database schema design as well as the ETL pipeline
9. images : Folder containing images used in this readme

## How to Run

1. Fill in the settings under the AWS and DWH tags in the configuration file `dwh.cfg` .
```
[CLUSTER]
host =
db_name =
db_user =
db_password =
db_port =

[IAM_ROLE]
arn =

[S3]
log_data = 's3://udacity-dend/log_data'
log_jsonpath = 's3://udacity-dend/log_json_path.json'
song_data = 's3://udacity-dend/song_data'

[AWS]
key =
secret =

[DWH]
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large
dwh_iam_role_name =
dwh_cluster_identifier =
dwh_db =
dwh_db_user =
dwh_db_password =
dwh_port = 5439
```

2. In the main directory, run `create_cluster.py` to create the Redshift clusters using the AWS python SDK boto3.

```
python -m create_cluster
```

3. Run `create_tables.py` to connect to the database and create the staging and analytics tables mentioned in the database schema. Running the script will also update the settings under the CLUSTER and IAM_ROLE tags in the configuration file `dwh.cfg` .

```
python -m create_tables
```

4. Run `etl.py` to load the data from S3 to the staging tables on Redshift, and then load the data from the staging tables to the analytics tables while still on Redshift.

```
python -m etl
```

5. Run `example_queries.py` to execute basic SQL queries for song play analysis to test the database and the ETL pipeline.

6. At the end of the project, you can delete the Redshift cluster by running `delete_cluster.py` .

```
python -m delete_cluster
```

## Sample Queries

Here are some basic examples of SQL queries for song play analysis and their results.

A. Query:
```
SELECT * FROM songplays LIMIT 5;
```

A. Result:
```
(37, datetime.datetime(2018, 11, 7, 17, 49, 16, 796000), 15, 'paid', None, None, 221, 'Chicago-Naperville-Elgin, IL-IN-WI', '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"')
(69, datetime.datetime(2018, 11, 6, 20, 54, 40, 796000), 97, 'paid', None, None, 293, 'Lansing-East Lansing, MI', '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"')
```

B. Query:
```
SELECT * FROM users LIMIT 5;
```

B. Result:
```
(101, 'Jayden', 'Fox', 'M', 'free')
(51, 'Maia', 'Burke', 'F', 'free')
```

C. Query:
```
SELECT * FROM songs LIMIT 5;
```

C. Result:
```
('SOKOZQI12A8C13C538', 'Shortkut', 'AR3HS9C1187FB48878', 1998, 151.71873)
('SOLGZML12AB018748D', 'Edith And The Kingpin', 'ARI1KP51187FB473B5', 0, 210.83383)
```

D. Query:
```
SELECT * FROM artists LIMIT 5;
```

D. Result:
```
('ARBUHDB1187FB3E72C', 'Bobby Vee', 'Fargo, ND', 46.87591, -96.78176)
('ARTDQRC1187FB4EFD4', 'Black Eyed Peas / Les Nubians / Mos Def', 'Los Angeles, CA', None, None)
```

E. Query:
```
SELECT * FROM time LIMIT 5;
```

E. Result:
```
(datetime.datetime(2018, 11, 3, 14, 17, 50, 796000), 14, 3, 44, 11, 2018, 6)
(datetime.datetime(2018, 11, 3, 16, 10, 32, 796000), 16, 3, 44, 11, 2018, 6)
```
