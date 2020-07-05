from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load any JSON formatted files from S3 to Amazon Redshift
    
    :param redshift_conn_id             The Redshift connection id
    :type redshift_conn_id              string
    :param aws_credentials_id           The AWS Access Key ID and AWS
                                        Secret Key
    :type aws_credentials_id            string
    :param table                        The SQL table name
    :type table                         string
    :param s3_bucket                    The S3 bucket name
    :type s3_bucket                     string
    :param s3_key                       The key or unique identifier to
                                        an object in an S3 bucket
    :type s3_key                        string
    :param region                       The AWS Region where AWS S3 stores
                                        the buckets
    :type region                        string
    :param json_option                  The method to map the data elements 
                                        in the JSON source data to the 
                                        columns in the target table
    :type json_option                   string
    :param time_format                  The time format
    :type time_option                   string
    """
    
    ui_color = '#358140'
    copy_sql = """
        COPY {table}
        FROM '{path}'
        CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        REGION AS {region}
        FORMAT AS JSON {json_option}
        TIMEFORMAT AS {time_format};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="'us-west-2'",
                 json_option="'auto'",
                 time_format="'auto'",
                 execution_date=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        self.time_format = time_format
        self.execution_date = execution_date

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         self.log.info(f"Clearing data from {self.table} table...")
#         redshift.run("DELETE FROM {}".format(self.table))
#         self.log.info(f"Clearing data from {self.table} table is completed")
        
        self.log.info("Copying data from S3 to Redshift...")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            region=self.region,
            json_option=self.json_option,
            time_format=self.time_format
        )
        redshift.run(formatted_sql)
        
        self.log.info("Copying of data from S3 to Redshift is completed")