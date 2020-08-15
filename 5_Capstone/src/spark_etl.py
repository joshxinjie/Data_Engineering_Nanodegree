import re
import os
import argparse

import boto3
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as R,\
    StructField as Fld, DoubleType as Dbl, StringType as Str,\
    IntegerType as Int, DateType as Date, TimestampType as TimeStamp

def process_argument():
    """
    Retrieves and parse the command line arguments.
    """
    parser = argparse.ArgumentParser()

    # Default argument
    parser.add_argument(
        "aws_access_key_id",\
        type=str,\
        help="Your AWS access key ID")
    parser.add_argument(
        "aws_secret_access_key",\
        type=str,\
        help="Your AWS secret access key"
    )
    parser.add_argument(
        "s3_bucket_raw_data",\
        type=str,\
        help="S3 bucket containing the raw data"
    )
    parser.add_argument(
        "s3_bucket_transformed_datalake",\
        type=str,\
        help="S3 bucket containing the transformed data"
    )
    parser.add_argument(
        "--immigration_data_folder_name",\
        type=str, default="18-83510-I94-Data-2016",\
        help="The name of the immigration data folder in S3"
    )
    parser.add_argument(
        "--us_cities_demog_filename",\
        type=str,\
        default="us-cities-demographics.csv",\
        help="The name of the us cities demographics file in S3"
    )
    parser.add_argument(
        "--immigration_data_dictionary_filename",\
        type=str,\
        default="I94_SAS_Labels_Descriptions.SAS",\
        help="The name of the immigration data dictionary file in S3"
    )

    args = parser.parse_args()
    
    return args

def create_client(service, region, access_key_id, secret_access_key):
    """
    Create clients for AWS resources.
    
    @type service: str
    @type region: str
    @type aws_access_key_id: str
    @type aws_secret_access_key: str
    @rtype client: boto3 client
    """
    client = boto3.client(service,\
                          region_name=region,\
                          aws_access_key_id=access_key_id,\
                          aws_secret_access_key=secret_access_key
                          )
    return client

def create_spark_session():
    """
    Creates a SparkSession
    
    @rtype spark: SparkSession
    """
    spark = SparkSession.builder.\
            config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark

# def generate_immigration_schema(template_immigration_df_path, spark):
#     """
#     template_immigration_df_path="../../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat"
#     """
#     imm_data_apr16_spark = spark.read.format('com.github.saurfang.sas.spark').load(templat_immigration_df_path)
#     schema_columns = imm_data_apr16_spark.schema.names
    
#     return schema_columns

# def read_raw_immigration_data(s3_data_path, spark, columns):
#     """
#     s3_data_path=../../data/18-83510-I94-Data-2016
#     """
#     files = os.listdir(s3_data_path)

#     for count, file in enumerate(files):
#         file_path = os.path.join(s3_data_path, file)
#         if count == 0:
#             raw_imm_df = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
#             raw_imm_df = raw_imm_df.select(columns)
#         else:
#             df = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
#             df = df.select(columns)
#             raw_imm_df = raw_imm_df.union(df)

#     # add immigration id
#     raw_imm_df = raw_imm_df.withColumn("immigration_id", F.monotonically_increasing_id())

#     return raw_imm_df

def read_raw_immigration_data(s3_bucket, s3_client, spark, raw_imm_table_columns, raw_imm_folder_prefix="18-83510-I94-Data-2016/"):
    """
    Prefix="18-83510-I94-Data-2016/"
    """
    files = []

    for key in s3_client.list_objects(Bucket=s3_bucket, Prefix=raw_imm_folder_prefix)['Contents']:
        files.append(key['Key'])

    s3_data_path = "".join(["s3a://", s3_bucket, "/"])

    for count, file in enumerate(files):
        file_path = os.path.join(s3_data_path, file)
        if count == 0:
            raw_imm_df = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
            raw_imm_df = raw_imm_df.select(raw_imm_table_columns)
        else:
            df = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
            df = df.select(raw_imm_table_columns)
            raw_imm_df = raw_imm_df.union(df)

    # add immigration id
    raw_imm_df = raw_imm_df.withColumn("immigration_id", F.monotonically_increasing_id())

    return raw_imm_df

def generate_immigration_fact_table(raw_imm_df):
    imm_fact_table_col = ["immigration_id", "fltno", "visatype", "i94port", "i94yr", "i94mon"]

    imm_fact_table = raw_imm_df.select(imm_fact_table_col)\
                        .dropDuplicates()\
                        .withColumnRenamed("fltno","flight_number")\
                        .withColumnRenamed("visatype","visa_type")\
                        .withColumnRenamed("i94port","us_port_of_arrival_code")\
                        .withColumn("_year", raw_imm_df.i94yr.cast(Int()))\
                        .drop("i94yr")\
                        .withColumn("_month", raw_imm_df.i94mon.cast(Int()))\
                        .drop("i94mon")
    
    ordered_col = ["immigration_id", "flight_number", "visa_type", "us_port_of_arrival_code", "_year", "_month"]

    imm_fact_table = imm_fact_table.select(ordered_col)

    return imm_fact_table

def generate_flight_dimension_table(raw_imm_df):
    flight_dim_table_col = ["fltno", "airline"]

    flight_dim_table = raw_imm_df.select(flight_dim_table_col)\
                            .dropDuplicates()\
                            .withColumnRenamed("fltno","flight_number")\
                            .where(F.col("flight_number").isNotNull())
    return flight_dim_table

def generate_visitor_dimension_table(raw_imm_df):
    visitor_dim_table_col = ["immigration_id", "biryear", "occup", "i94res"]

    visitor_dim_table = raw_imm_df.select(visitor_dim_table_col)\
                            .dropDuplicates()\
                            .withColumn("birth_year", raw_imm_df.biryear.cast(Int())).drop("biryear")\
                            .withColumnRenamed("occup","occupation")\
                            .withColumn("country_of_residence", raw_imm_df.i94res.cast(Int())).drop("i94res")

    ordered_col = ["immigration_id", "birth_year", "occupation", "country_of_residence"]

    visitor_dim_table = visitor_dim_table.select(ordered_col)

    return visitor_dim_table

def generate_trip_records_dimension_table(raw_imm_df):
    trip_records_dim_table_col = [
        "immigration_id", "arrdate", "i94yr", "i94mon", "i94cit",\
        "depdate", "dtadfile", "entdepa", "entdepd",\
        "matflag", "dtaddto", "visapost"
    ]

    trip_records_dim_table = raw_imm_df.select(trip_records_dim_table_col)\
                                .dropDuplicates()\
                                .withColumn("arrival_date", raw_imm_df.arrdate.cast(Int()))\
                                .drop("arrdate")\
                                .withColumn("year_of_arrival", raw_imm_df.i94yr.cast(Int()))\
                                .drop("i94yr")\
                                .withColumn('_year', F.col("year_of_arrival"))\
                                .withColumn("month_of_arrival", raw_imm_df.i94mon.cast(Int()))\
                                .drop("i94mon")\
                                .withColumn('_month', F.col("month_of_arrival"))\
                                .withColumn("country_of_prev_depart", raw_imm_df.i94cit.cast(Int()))\
                                .drop("i94cit")\
                                .withColumn("depart_date", raw_imm_df.depdate.cast(Int()))\
                                .drop("depdate")\
                                .withColumn("date_of_file_entry", raw_imm_df.dtadfile.cast(Int()))\
                                .drop("dtadfile")\
                                .withColumnRenamed("entdepa","arrival_flag")\
                                .withColumnRenamed("entdepd","departure_flag")\
                                .withColumnRenamed("matflag","match_flag")\
                                .withColumn("last_permitted_day_of_stay", raw_imm_df.dtaddto.cast(Int()))\
                                .drop("dtaddto")\
                                .withColumnRenamed("visapost","state_of_visa_issued")\

    ordered_col = [
        "immigration_id", "arrival_date", "year_of_arrival", "month_of_arrival", "country_of_prev_depart",\
        "depart_date", "date_of_file_entry", "arrival_flag", "departure_flag", "match_flag",\
        "last_permitted_day_of_stay", "state_of_visa_issued", "_year", "_month"
    ]

    trip_records_dim_table = trip_records_dim_table.select(ordered_col)

    return trip_records_dim_table

def generate_visa_dim_table(raw_imm_df):
    visa_dim_table_col = ["visatype", "i94visa"]

    visa_dim_table = raw_imm_df.select(visa_dim_table_col)\
                        .dropDuplicates()\
                        .withColumnRenamed("visatype","visa_type")\
                        .withColumnRenamed("i94visa","visa_category")\
                        .where(F.col("visa_type").isNotNull())
    
    return visa_dim_table

def write_table_to_parquet_in_s3(table_df, table_name, s3_output_bucket_path, partition_by_year_month=False):
    # flight_dimension.parquet
    full_table_name = "".join([table_name, ".parquet"])
    if partition_by_year_month:
        table_df.write.mode('overwrite')\
            .partitionBy("_year", "_month")\
            .parquet(os.path.join(s3_output_bucket_path, full_table_name))
    else:
        table_df.write.mode('overwrite')\
            .parquet(os.path.join(s3_output_bucket_path, full_table_name))


def extract_new_columns(original_column):
    new_columns = original_column.split(";")
    # lower-case column names
    new_columns = [new_col.lower() for new_col in new_columns]
    # replace space with underscore in column names
    new_columns = [new_col.replace(" ", "_") for new_col in new_columns]
    return new_columns

def clean_cities_demog_table(us_cities_demog_df):
    original_column = us_cities_demog_df.columns[0]
    new_columns = extract_new_columns(original_column)
    
    for i, single_new_col in enumerate(new_columns):
        if i == 0:
            new_us_cities_demog_df = us_cities_demog_df.withColumn(single_new_col, F.split(F.col(original_column), ";").getItem(i))
        else:
            new_us_cities_demog_df = new_us_cities_demog_df.withColumn(single_new_col, F.split(F.col(original_column), ";").getItem(i))
    new_us_cities_demog_df = new_us_cities_demog_df.drop(original_column)
    new_us_cities_demog_df = new_us_cities_demog_df.withColumn("city_id", F.monotonically_increasing_id())
    
    return new_us_cities_demog_df

def extract_sub_df(df, columns):
    sub_df = df.select(columns).dropDuplicates()
    return sub_df

def extract_city_and_state(city_state_str):
    if "," in city_state_str:
        city, state = city_state_str.split(",")[0].strip(),\
                        city_state_str.split(",")[1].strip()
    else:
        city = city_state_str
        state = np.nan
    city = city.title()
    return city, state

def extract_ports_and_cities(txt_file, schema, spark):
    processing_i94port = False

    ports_and_cities_df_values = []
    
    unwanted_chars = r'[^a-zA-Z0-9(), ]'

    for line in txt_file:
        if "I94PORT" in line:
            processing_i94port = True
        if processing_i94port and "=" in line:
            i94port, city_w_state = line.split("=")[0], line.split("=")[1]
            # remove unwanted characters from string
            i94port = re.sub(unwanted_chars, '', i94port).strip()
            city_w_state = re.sub(unwanted_chars, '', city_w_state)
            city, state = extract_city_and_state(city_w_state)
            ports_and_cities_df_values.append((i94port, city))
        if processing_i94port and (";" in line):
            processing_i94port = False
            break
    
    ports_and_cities_df = spark.createDataFrame(ports_and_cities_df_values, schema=schema)
    
    return ports_and_cities_df


def main():
    args = process_argument()

    os.environ['AWS_ACCESS_KEY_ID']=args.aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY']=args.aws_secret_access_key

    s3_bucket_raw_data = args.s3_bucket_raw_data
    s3_bucket_transformed_datalake = args.s3_bucket_transformed_datalake
    us_cities_demog_filename = args.us_cities_demog_filename
    immigration_data_dictionary_filename = args.immigration_data_dictionary_filename

    # create boto3 s3 client
    s3_client = create_client(
                    "s3",\
                    region="us-west-2",\
                    access_key_id=args.aws_access_key_id,\
                    secret_access_key=args.aws_secret_access_key
                )

    spark = create_spark_session()

    # s3_data_path = s3://udend-capstone-data-xj/18-83510-I94-Data-2016/*.sas7bdat
    # raw_imm_data_s3_path = "".join([
    #     "s3://",\
    #     s3_bucket_raw_data,\
    #     "/18-83510-I94-Data-2016"
    # ])
    #raw_imm_data_s3_path = "../../data/18-83510-I94-Data-2016"
    
    # raw_imm_data = read_raw_immigration_data(
    #                     s3_data_path=raw_imm_data_s3_path,\
    #                     spark=spark,\
    #                     columns=raw_imm_data_schema
    #                 )
    raw_immigration_folder_name_in_s3 = args.immigration_data_folder_name
    raw_immigration_folder_prefix = "".join([raw_immigration_folder_name_in_s3, "/"])

    raw_imm_data_schema = [
        'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',\
        'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate',\
        'i94bir', 'i94visa', 'count', 'dtadfile', 'visapost',\
        'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag',\
        'biryear', 'dtaddto', 'gender', 'insnum', 'airline',\
        'admnum', 'fltno', 'visatype'
    ]
    raw_imm_data = read_raw_immigration_data(
                        s3_bucket=s3_bucket_raw_data,\
                        s3_client=s3_client,\
                        spark=spark,\
                        raw_imm_table_columns=raw_imm_data_schema,\
                        raw_imm_folder_prefix=raw_immigration_folder_prefix
                    )
    
    us_cities_data_path_in_s3 = "".join(["s3a://", s3_bucket_raw_data, "/", us_cities_demog_filename])
    us_cities_demog_table = spark.read.option("header",True).csv(us_cities_data_path_in_s3)

    s3_client.download_file(Bucket=s3_bucket_raw_data, Key=immigration_data_dictionary_filename, Filename=immigration_data_dictionary_filename)
    immigration_data_dictionary = open(immigration_data_dictionary_filename, "r")
    
    # s3_output_path = s3://udend-capstone-datalake-xj
    transformed_tables_output_path_in_s3 = "".join(["s3://", s3_bucket_transformed_datalake])
    #transformed_tables_output_path_in_s3 = "output_data"
    
    imm_fact_table = generate_immigration_fact_table(raw_imm_df=raw_imm_data)
    write_table_to_parquet_in_s3(
        table_df=imm_fact_table,\
        table_name="immigration",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=True
    )
    
    flight_dim_table = generate_flight_dimension_table(raw_imm_df=raw_imm_data)
    write_table_to_parquet_in_s3(
        table_df=flight_dim_table,\
        table_name="flight",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=False
    )
    
    visitor_dim_table = generate_visitor_dimension_table(raw_imm_df=raw_imm_data)
    write_table_to_parquet_in_s3(
        table_df=visitor_dim_table,\
        table_name="visitor",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=False
    )

    trip_records_dim_table = generate_trip_records_dimension_table(raw_imm_df=raw_imm_data)
    write_table_to_parquet_in_s3(
        table_df=trip_records_dim_table,\
        table_name="trip_records",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=True
    )

    visa_dim_table = generate_visa_dim_table(raw_imm_df=raw_imm_data)
    write_table_to_parquet_in_s3(
        table_df=visa_dim_table,\
        table_name="visa",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=False
    )

    seperated_us_cities_demog = clean_cities_demog_table(us_cities_demog_table)

    ports_and_cities_schema = R([
        Fld("us_port_of_arrival_code", Str()),
        Fld("city", Str())
    ])
    ports_and_cities_table = extract_ports_and_cities(
        txt_file=immigration_data_dictionary,\
        schema=ports_and_cities_schema,\
        spark=spark
    )

    us_cities_demog_w_port_of_arrival_code = ports_and_cities_table.join(
                                                seperated_us_cities_demog,\
                                                ["city"]
                                            )

    general_demog_cols = [
        "us_port_of_arrival_code", "city", "state", "median_age", "male_population",\
        "female_population", "total_population", "number_of_veterans",\
        "foreign-born", "average_household_size", "state_code"
    ]
    us_cities_general_demog_table = extract_sub_df(
        df=us_cities_demog_w_port_of_arrival_code,\
        columns=general_demog_cols
    )
    write_table_to_parquet_in_s3(
        table_df=us_cities_general_demog_table,\
        table_name="us_cities_general_demog",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=False
    )

    race_demog_cols = [
        "us_port_of_arrival_code", "race", "count"
    ]
    us_cities_race_demog_table = extract_sub_df(
        df=us_cities_demog_w_port_of_arrival_code,\
        columns=race_demog_cols
    )
    write_table_to_parquet_in_s3(
        table_df=us_cities_race_demog_table,\
        table_name="us_cities_race_demog",\
        s3_output_bucket_path=transformed_tables_output_path_in_s3,\
        partition_by_year_month=False
    )

if __name__ == "__main__":
    main()