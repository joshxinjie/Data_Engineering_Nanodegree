import os
import configparser
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl,\
                                StringType as Str, IntegerType as Int, DateType as Date,\
                                TimestampType as TimeStamp

def create_spark_session():
    """
    Creates a SparkSession
    
    @rtype spark: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Load the song metadata from s3, and create the songs and artists analytics tables
    and store them in s3
    
    @type spark: SparkSession
    @type input_data: str
    @type output_data: str
    @rtype None
    """
    # get filepath to song data file
    # read all json files
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # set schema for song_data json files
    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table
    song_table_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(song_table_cols).dropDuplicates()

    songs_table.withColumn('_year', F.col("year"))\
                .withColumn('_artist_id', F.col("artist_id"))\
                .write.mode('overwrite')\
                .partitionBy("_year", "_artist_id")\
                .parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns to create artists table
    artist_table_cols = ["artist_id",\
                         "artist_name",\
                         "artist_location",\
                         "artist_latitude",\
                         "artist_longitude"]
    artists_table = df.select(artist_table_cols)\
                        .dropDuplicates()\
                        .withColumnRenamed("artist_name","name")\
                        .withColumnRenamed("artist_location","location")\
                        .withColumnRenamed("artist_latitude","latitude")\
                        .withColumnRenamed("artist_longitude","longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, "artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    Load the user activity logs from s3 as well as the songs analytics table,
    and create the users, time and songplay analytics tables and store them in s3
    
    @type spark: SparkSession
    @type input_data: str
    @type output_data: str
    @rtype None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    #log_data = os.path.join(input_data, "log_data/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table_cols = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = df.select(users_table_cols)\
                            .dropDuplicates()\
                            .withColumnRenamed("userId","user_id")\
                            .withColumnRenamed("firstName","first_name")\
                            .withColumnRenamed("lastName","last_name")\
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x : x / 1000)
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x : datetime.fromtimestamp(x), Date())
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select([
        F.col("timestamp").alias("start_time"),\
        F.hour("datetime").alias("hour"),\
        F.dayofmonth("datetime").alias("day"),\
        F.weekofyear("datetime").alias("week"),\
        F.month("datetime").alias("month"),\
        F.year("datetime").alias("year"),\
        F.date_format("datetime", "E").alias("weekday")#u for day number, E for day string
    ]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite')\
                .partitionBy("year", "month")\
                .parquet(os.path.join(output_data, "time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs.parquet/*/*/*.parquet"))
    #song_df = spark.read.parquet(os.path.join(output_data, "songs.parquet"))
    
    log_song_df = df.join(song_df, df.song == song_df.title)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_song_df.select([
        F.monotonically_increasing_id().alias('songplay_id'),\
        F.col('datetime').alias("start_time"),\
        F.year('datetime').alias('year'),\
        F.month('datetime').alias('month'),\
        F.col("userId").alias("user_id"),\
        "level",\
        "song_id",\
        "artist_id",\
        F.col('sessionId').alias("session_id"),\
        "location",\
        F.col('userAgent').alias("user_agent"),\
    ])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.dropDuplicates()\
                    .withColumn('_year', F.col("year"))\
                    .withColumn('_month', F.col("month"))\
                    .write.mode('overwrite')\
                    .partitionBy("_year", "_month")\
                    .parquet(os.path.join(output_data, "songplays.parquet"))


def main():
    config = configparser.ConfigParser()
    #config.read('dl.cfg')
    
    config.read('/home/hadoop/dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    #input_data = "data"
    #output_data = "output"
    
    input_data=config['S3']['INPUT_BUCKET']
    output_data="".join(["s3://", config['S3']['OUTPUT_BUCKET_NAME']])
    print("Reading in data from: {}".format(input_data))
    print("Exporting results to: {}".format(output_data))
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
