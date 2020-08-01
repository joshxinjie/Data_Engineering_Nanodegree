import os
import re
import argparse

import numpy as np
import pandas as pd
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
    parser.add_argument("aws_access_key_id", type=str, help="Your AWS access key ID")
    parser.add_argument("aws_secret_access_key", type=str, help="Your AWS secret access key")
    parser.add_argument("raw_data_s3_bucket", type=str, help="S3 bucket containing the raw data")
    parser.add_argument("transformed_data_s3_bucket", type=str, help="S3 bucket containing the transformed data")

    args = parser.parse_args()
    
    # For checking purposes
    print(args.aws_access_key_id)
    print(args.aws_secret_access_key)
    
    return args

def create_spark_session():
    """
    Creates a SparkSession
    
    @rtype spark: SparkSession
    """
    spark = SparkSession.builder.\
            config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark

def read_raw_immigration_data(s3_data_path, spark):
    """
    """

    raw_imm_df = spark.read.format('com.github.saurfang.sas.spark').load(s3_data_path)

    # add immigration id
    raw_imm_df = raw_imm_df.withColumn("immigration_id", F.monotonically_increasing_id())

    return raw_imm_df

def generate_immigration_fact_table(raw_imm_df):
    imm_fact_table_col = ["immigration_id", "fltno", "visatype"]

    imm_fact_table = raw_imm_df.select(imm_fact_table_col)\
                        .dropDuplicates()\
                        .withColumnRenamed("fltno","flight_number")\
                        .withColumnRenamed("visatype","visa_type")\
                        .withColumn("year_of_arrival", raw_imm_df.i94yr.cast(Int()))\
                        .drop("i94yr")\
                        .withColumn('_year', F.col("year_of_arrival"))\
                        .withColumn("month_of_arrival", raw_imm_df.i94mon.cast(Int()))\
                        .drop("i94mon")\
                        .withColumn('_month', F.col("month_of_arrival"))
    return imm_fact_table

def write_immigration_fact_table_to_s3(imm_fact_table, s3_output_bucket_path):
    """
    """
    # write immigration fact table to parquet files
    imm_fact_table.write.mode('overwrite')\
        .partitionBy("_year", "_month")\
        .parquet(os.path.join(s3_output_bucket_path, "immigration.parquet"))

def main():
    args = process_argument()

    os.environ['AWS_ACCESS_KEY_ID']=args.aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY']=args.aws_secret_access_key

    spark = create_spark_session()

    # s3_data_path = s3://udend-capstone-data-xj/18-83510-I94-Data-2016/*.sas7bdat
    raw_imm_data_s3_path = "".join(["s3://", args.raw_data_s3_bucket, "/18-83510-I94-Data-2016/*.sas7bdat", ])
    raw_imm_data = read_raw_immigration_data(
                        s3_data_path=raw_imm_data_s3_path,\
                        spark=spark
                    )
    
    # s3_output_path = s3://udend-capstone-datalake-xj/
    imm_fact_table_s3_path = "".join(["s3://", args.transformed_data_s3_bucket, "/"])
    imm_fact_table = generate_immigration_fact_table(raw_imm_df=raw_imm_data)
    write_immigration_fact_table_to_s3(
        imm_fact_table=imm_fact_table,\
        s3_output_bucket_path=imm_fact_table_s3_path
    )

    