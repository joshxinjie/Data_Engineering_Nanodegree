from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class CreateS3BucketOperator(BaseOperator):
    
    @apply_defaults
    def __init__(
        self,\
        aws_credentials_id,\
        s3_bucket,\
        region,\
        *args, **kwargs
        ):

        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.region = region

    def execute(self, context):
        s3_hook = S3Hook(self.aws_credentials_id)

        if s3_hook.check_for_bucket(bucket_name=self.s3_bucket):
            self.log.info(f'S3 Bucket Already Exist \n Bucket Name: {self.s3_bucket} \n Region: {self.region}')
        else:
            try:
                s3_hook.create_bucket(bucket_name=self.s3_bucket, region_name=self.region)
                self.log.info(f'S3 Bucket Created \n Bucket Name: {self.s3_bucket} \n Region: {self.region}')
            except:
                self.log.info(f'Unable to Create S3 Bucket \n Bucket Name: {self.s3_bucket} \n Region: {self.region}')