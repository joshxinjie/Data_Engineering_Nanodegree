import os
import argparse

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as R,\
    StructField as Fld, DoubleType as Dbl, StringType as Str,\
    IntegerType as Int, DateType as Date, TimestampType as TimeStamp

RAW_IMM_DATA_SCHEMA = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',\
                       'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate',\
                       'i94bir', 'i94visa', 'count', 'dtadfile', 'visapost',\
                       'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag',\
                       'biryear', 'dtaddto', 'gender', 'insnum', 'airline',\
                       'admnum', 'fltno', 'visatype']

def process_argument():
    """
    Retrieves and parse the command line arguments.
    """
    parser = argparse.ArgumentParser()

    # Default argument
    parser.add_argument("aws_access_key_id", type=str, help="Your AWS access key ID")
    parser.add_argument("aws_secret_access_key", type=str, help="Your AWS secret access key")
    parser.add_argument("s3_bucket_raw_data", type=str, help="S3 bucket containing the raw data")
    parser.add_argument("s3_bucket_transformed_datalake", type=str, help="S3 bucket containing the transformed data")

    args = parser.parse_args()
    
    # For checking purposes
    print(args.aws_access_key_id)
    print(args.aws_secret_access_key)
    
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
    imm_fact_table_col = ["immigration_id", "fltno", "visatype", "i94yr", "i94mon"]

    imm_fact_table = raw_imm_df.select(imm_fact_table_col)\
                        .dropDuplicates()\
                        .withColumnRenamed("fltno","flight_number")\
                        .withColumnRenamed("visatype","visa_type")\
                        .withColumn("_year", raw_imm_df.i94yr.cast(Int()))\
                        .drop("i94yr")\
                        .withColumn("_month", raw_imm_df.i94mon.cast(Int()))\
                        .drop("i94mon")
    
    ordered_col = ["immigration_id", "flight_number", "visa_type", "_year", "_month"]

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
        "i94port", "depdate", "dtadfile", "entdepa", "entdepd",\
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
                                .withColumnRenamed("i94port","port_of_arrival_code")\
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
        "port_of_arrival_code", "depart_date", "date_of_file_entry", "arrival_flag", "departure_flag",\
        "match_flag", "last_permitted_day_of_stay", "state_of_visa_issued", "_year", "_month"
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


def main():
    args = process_argument()

    os.environ['AWS_ACCESS_KEY_ID']=args.aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY']=args.aws_secret_access_key

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
    #     args.s3_bucket_raw_data,\
    #     "/18-83510-I94-Data-2016"
    # ])
    #raw_imm_data_s3_path = "../../data/18-83510-I94-Data-2016"
    
    # raw_imm_data = read_raw_immigration_data(
    #                     s3_data_path=raw_imm_data_s3_path,\
    #                     spark=spark,\
    #                     columns=RAW_IMM_DATA_SCHEMA
    #                 )

    raw_imm_data = read_raw_immigration_data(
                        s3_bucket=args.s3_bucket_raw_data,\
                        s3_client=s3_client,\
                        spark=spark,\
                        raw_imm_table_columns=RAW_IMM_DATA_SCHEMA
                    )
    
    # s3_output_path = s3://udend-capstone-datalake-xj
    transformed_tables_output_path_in_s3 = "".join(["s3://", args.s3_bucket_transformed_datalake])
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

if __name__ == "__main__":
    main()