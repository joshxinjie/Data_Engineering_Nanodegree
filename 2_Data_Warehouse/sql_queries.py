import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events \
    (artist VARCHAR,
    auth VARCHAR,
    firstNAME VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs \
    (num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay \
    (songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP DISTKEY,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users \
    (user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs \
    (song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists \
    (artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time \
    (start_time timestamp PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {data_bucket}
    CREDENTIALS 'aws_iam_role={iam_role}'
    REGION AS 'us-west-2'
    FORMAT AS JSON {log_json_path}
    TIMEFORMAT as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], iam_role=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {data_bucket}
    CREDENTIALS 'aws_iam_role={iam_role}'
    REGION AS 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], iam_role=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT e.ts AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        (SELECT DISTINCT userId as user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
        FROM staging_events
        WHERE user_id IS NOT NULL
        AND page = 'NextSong')
    ON CONFLICT (user_id)
    DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        (SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        FROM staging_songs
        WHERE song_id IS NOT NULL)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        (SELECT DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL)
    ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        (SELECT DISTINCT ts AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(dayofweek FROM ts) AS weekday
        FROM staging_events
        WHERE ts IS NOT NULL)
    ON CONFLICT (start_time) DO NOTHING;
""")

# Example queries for analytics tables

get_top_5_songplays = ("""
    SELECT * 
    FROM songplays
    LIMIT 5;
""")

get_top_5_users = ("""
    SELECT * 
    FROM users
    LIMIT 5;
""")

get_top_5_songs = ("""
    SELECT * 
    FROM songs
    LIMIT 5;
""")

get_top_5_artists = ("""
    SELECT * 
    FROM artists
    LIMIT 5;
""")

get_top_5_time = ("""
    SELECT * 
    FROM time
    LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytics_table_example_queries = [get_top_5_songplays, get_top_5_users, get_top_5_songs, get_top_5_artists, get_top_5_time]