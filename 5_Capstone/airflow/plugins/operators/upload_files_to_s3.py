import os
import glob

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class UploadFilesToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(
        self,\
        aws_credentials_id,\
        source_folder,\
        s3_target_folder,\
        s3_bucket,\
        file_extension = None,\
        *args, **kwargs
        ):
        
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.source_folder = source_folder
        self.s3_target_folder = s3_target_folder
        self.s3_bucket = s3_bucket
        self.file_extension = file_extension
 
    def execute(self, context):
        s3_hook = S3Hook(self.aws_credentials_id)

        if self.file_extension:
            files = glob.glob(os.path.join(self.source_folder, self.file_extension))
        else:
            files = os.listdir(self.source_folder)
            files = [file for file in files if os.path.isfile(os.path.join(self.source_folder, file))]
        print("Files: ")
        print(files)
        print(os.listdir(os.getcwd()))
        for filepath in files:
            try:
                filename = filepath.split("/")[1]
                if self.s3_target_folder:
                    s3_key = "".join([self.s3_target_folder, "/", filename])
                else:
                    s3_key = filename
                self.log.info(f"Uploading \n File: {filename} \n S3 Key: {s3_key} \n S3 Bucket: {self.s3_bucket}")
                #s3_hook.load_file(filename=filepath, key=filename, bucket_name=self.s3_bucket)
                s3_hook.load_file(filename=filepath, key=s3_key, bucket_name=self.s3_bucket, replace=True)
                self.log.info(f"Uploaded \n File: {filename} \n S3 Key: {s3_key} \n S3 Bucket: {self.s3_bucket}")
            except:
                self.log.info(f"Unable to Upload \n File: {filename} \n S3 Key: {s3_key} \n S3 Bucket: {self.s3_bucket}")