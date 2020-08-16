import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    """
    Creates a SparkSession
    
    @rtype spark: SparkSession
    """
    spark = SparkSession.builder.\
            config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark

def process_argument():
    """
    Retrieves and parse the command line arguments.
    """
    parser = argparse.ArgumentParser()

    # Default argument
    parser.add_argument("aws_access_key_id", type=str, help="Your AWS access key ID")
    parser.add_argument("aws_secret_access_key", type=str, help="Your AWS secret access key")
    parser.add_argument("s3_bucket_transformed_datalake", type=str, help="S3 bucket containing the transformed data")

    args = parser.parse_args()
    
    return args

def check_empty_table(spark_df, spark_df_name):
    """
    Check if the Spark DataFrame is empty.

    @type spark_df: Spark DataFrame
    @type spark_df_name: str
    @rtype None
    """
    if spark_df.count() == 0:
        raise AssertionError("FAIL: {} is empty".format(spark_df_name))
    else:
        print("PASS: Table {} pass empty table check".format(spark_df_name))

def count_null_values_by_column(spark_df, columns_to_check):
    """
    Returns a dictionary that counts the number of null values in each column
    of the Spark DataFrame.

    An example of the dictionary:

    {"flight_number": 0, "visa_type": 3}

    @type spark_df: Spark DataFrame
    @type columns_to_check: List of str
    @rtype col_nullcnt_dict: dict
    """
    col_nullcnt_dict = {}
    null_counts_df = spark_df.select([F.count(F.when(F.isnan(col), col)).alias(col) for col in columns_to_check])
    for index, col in enumerate(columns_to_check):
        col_nullcnt_dict[col] = null_counts_df.first()[index]
    
    return col_nullcnt_dict

def check_table_columns_for_null_values(spark_df, spark_df_name, columns_to_check):
    """
    Check a Spark DataFrame for the presence of null values in the specified columns.

    @type spark_df: Spark DataFrame
    @type spark_df_name: str
    @type columns_to_check: List of str
    @rtype None
    """
    null_present = False
    print("Checking table {} for null values".format(spark_df_name))

    columns_nullcnts_dict = count_null_values_by_column(spark_df, columns_to_check)

    for col in columns_nullcnts_dict:
        col_null_count = columns_nullcnts_dict[col]
        print("Column {} has {} null values".format(col, str(col_null_count)))
        if col_null_count > 0:
            null_present = True
    
    if null_present:
        raise AssertionError("FAIL: {} failed null column check".format(spark_df_name))
    else:
        print("PASS: Table {} pass null values check".format(spark_df_name))

def main():
    args = process_argument()

    os.environ['AWS_ACCESS_KEY_ID']=args.aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY']=args.aws_secret_access_key

    spark = create_spark_session()

    s3_bucket_datalake_path = "".join(["s3a://", args.s3_bucket_transformed_datalake])

    imm_fact_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "immigration.parquet"))
    flight_dim_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "flight.parquet"))
    visitor_dim_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "visitor.parquet"))
    trip_records_dim_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "trip_records.parquet"))
    visa_dim_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "visa.parquet"))
    us_cities_general_demog_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "us_cities_general_demog.parquet"))
    us_cities_race_demog_table = spark.read.parquet(os.path.join(s3_bucket_datalake_path, "us_cities_race_demog.parquet"))

    check_empty_table(spark_df=imm_fact_table, spark_df_name="immigration")
    check_empty_table(spark_df=flight_dim_table, spark_df_name="flight")
    check_empty_table(spark_df=visitor_dim_table, spark_df_name="visitor")
    check_empty_table(spark_df=trip_records_dim_table, spark_df_name="trip_records")
    check_empty_table(spark_df=visa_dim_table, spark_df_name="visa")
    check_empty_table(spark_df=us_cities_general_demog_table, spark_df_name="us_cities_general_demog")
    check_empty_table(spark_df=us_cities_race_demog_table, spark_df_name="us_cities_race_demog")

    imm_table_nonull_columns = ["immigration_id"]
    flight_table_nonull_columns = ["flight_number"]
    visitor_table_nonull_columns = ["immigration_id"]
    trip_records_table_nonull_columns = ["immigration_id"]
    visa_table_nonull_columns = ["visa_type"]
    us_cities_general_demog_nonull_columns = ["us_port_of_arrival_code"]
    us_cities_race_demog_nonull_columns = ["us_port_of_arrival_code"]

    check_table_columns_for_null_values(
        spark_df=imm_fact_table,\
        spark_df_name="immigration",\
        columns_to_check=imm_table_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=flight_dim_table,\
        spark_df_name="flight_number",\
        columns_to_check=flight_table_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=visitor_dim_table,\
        spark_df_name="visitor",\
        columns_to_check=visitor_table_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=trip_records_dim_table,\
        spark_df_name="trip_records",\
        columns_to_check=trip_records_table_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=visa_dim_table,\
        spark_df_name="visa",\
        columns_to_check=visa_table_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=us_cities_general_demog_table,\
        spark_df_name="us_cities_general_demog",\
        columns_to_check=us_cities_general_demog_nonull_columns
    )
    check_table_columns_for_null_values(
        spark_df=us_cities_race_demog_table,\
        spark_df_name="us_cities_race_demog",\
        columns_to_check=us_cities_race_demog_nonull_columns
    )
    
if __name__ == "__main__":
    main()