import os
import glob
import time
import json
import boto3
import configparser

def create_aws_client(service, access_key_id, secret_access_key, region='us-west-2'):
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

def create_s3_bucket(bucket_name, s3_client, region='us-west-2'):
    """
    Create an s3 bucket
    
    @type bucket_name: str
    @type s3_client: str
    @rtype client: None
    """
    try:
        print(f"Creating s3 bucket '{bucket_name}'")
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name,\
                                CreateBucketConfiguration=location)
    except Exception as e:
        print(f"Unable to create s3 bucket '{bucket_name}'. Bucket may have already been created.")
    
def upload_file_to_s3(file_path, bucket_name, s3_file_name, s3_client):
    """
    Upload file to an s3 bucket
    
    @type file_path: str
    @type bucket_name: str
    @type s3_file_name: str
    @type s3_client: str
    @rtype client: None
    """
    try:
        print(f"Uploading file '{file_path}' to s3 bucket '{bucket_name}' with the name '{s3_file_name}'")
        s3_client.upload_file(Filename=file_path,\
                              Bucket=bucket_name,\
                              Key=s3_file_name
                             )
    except Exception as e:
        print(f"Unable to upload file '{file_path}'")
    
def upload_folder_of_files_to_s3(folder_dir, file_ext, bucket_name, s3_folder_name, s3_client):
    """
    Upload a folder of files to an s3 bucket
    
    file_ext="*.sas7bdat"
    """
    files = glob.glob(os.path.join(folder_dir, file_ext))
    for filename in files:
        try:
            print(f"Uploading file {filename} to folder {s3_folder_name} in s3 bucket {bucket_name}")
            s3_file_name = f"{s3_folder_name}/{os.path.basename(filename)}"
            upload_file_to_s3(file_path=filename,\
                              bucket_name=bucket_name,\
                              s3_file_name=s3_file_name,\
                              s3_client=s3_client
                             )
        except:
            print(f"Unable to upload file '{filename}' to folder '{s3_folder_name}' in s3 bucket '{bucket_name}'")


def main():
    config = configparser.ConfigParser()
    config.read('config.cfg')    
    
    s3_client = create_aws_client("s3",\
                                  access_key_id=config['AWS_CRED']['AWS_ACCESS_KEY_ID'],\
                                  secret_access_key=config['AWS_CRED']['AWS_SECRET_ACCESS_KEY'],\
                                  region=config['AWS_CRED']['REGION']
                                 )
    
    # create s3 bucket for raw data files
    create_s3_bucket(bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                     s3_client=s3_client,\
                     region=config['AWS_CRED']['REGION']
                    )
    
    # upload sample immigration data
    upload_file_to_s3(file_path=config['DATA_DIR_PATHS']['OPTIONAL_SAMPLE_IMM_DATA_DIR'],\
                      bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                      s3_file_name=config['S3_DATA_BUCKET']['OPTIONAL_SAMPLE_IMM_DATA_FILE_NAME'],\
                      s3_client=s3_client
                     )
    
    # Upload full immigration data
    upload_folder_of_files_to_s3(folder_dir=config['DATA_DIR_PATHS']['IMM_DATA_DIR'],\
                                 file_ext="*.sas7bdat",\
                                 bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                                 s3_folder_name=config['S3_DATA_BUCKET']['FULL_IMM_DATA_DIR_NAME'],\
                                 s3_client=s3_client
                                )
    
    # upload temperature data to S3
    upload_file_to_s3(file_path=config['DATA_DIR_PATHS']['TEMP_DATA_DIR'],\
                      bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                      s3_file_name=config['S3_DATA_BUCKET']['TEMP_DATA_FILE_NAME'],\
                      s3_client=s3_client
                     )
        
    # upload cities demographics data to S3
    upload_file_to_s3(file_path=config['DATA_DIR_PATHS']['CITIES_DEMOG_DIR'],\
                      bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                      s3_file_name=config['S3_DATA_BUCKET']['CITIES_DEMOG_FILE_NAME'],\
                      s3_client=s3_client
                     )
    
    # upload airport codes data to S3
    upload_file_to_s3(file_path=config['DATA_DIR_PATHS']['AIRPORT_CODES_DIR'],\
                      bucket_name=config['S3_DATA_BUCKET']['BUCKET_NAME'],\
                      s3_file_name=config['S3_DATA_BUCKET']['AIRPORT_CODES_FILE_NAME'],\
                      s3_client=s3_client
                     )
    
if __name__ == '__main__':
    main()