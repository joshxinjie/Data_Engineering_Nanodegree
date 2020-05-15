import os
import time
import json
import boto3
import configparser

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

def create_s3_buckets(bucket_name, s3_client):
    """
    Create an s3 bucket
    
    @type bucket_name: str
    @type s3_client: str
    @rtype client: None
    """
    location = {'LocationConstraint': 'us-west-2'}
    s3_client.create_bucket(Bucket=bucket_name,\
                            CreateBucketConfiguration=location)
    
def upload_code(file_name, bucket_name, s3_client):
    """
    Upload file to an s3 bucket
    
    @type file_name: str
    @type bucket_name: str
    @type s3_client: str
    @rtype client: None
    """
    s3_client.upload_file(Filename=file_name,\
                          Bucket=bucket_name,\
                          Key=file_name
                         )

def create_iam_role(iam_client, iam_role_name):
    """
    Create an IAM Role that allows EMR to have full access to the S3 bucket
    
    @type iam_client: boto3 client
    @type iam_role_name: str
    @rtype role_arn: str
    """
    print('Creating a New IAM Role')
    role = iam_client.create_role(
        RoleName=iam_role_name,
        Description='Allows EMR to call AWS services on your behalf',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {
                    #"AWS":"arn:aws:iam::AWSAcctID:role/EMR_EC2_DefaultRole"
                    'Service': 'elasticmapreduce.amazonaws.com'
                }
            }]
        })
    )
    
    print('Attaching S3 Full Access Policy')
    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )
    
    print('Attaching Default EMR Policy')
    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    )
    
    print('Getting the IAM Role ARN')
    role_arn = iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
    
    print("IAM Role ARN created: "+str(role_arn))
    
    return role_arn

def create_emr_cluster(code_bucket_name, emr_client, emr_iam_role, log_bucket_name):
    """
    Creates the EMR cluster, run the ETL script, then terminate the cluster
    
    @type code_bucket_name: str
    @type emr_client: boto3 client
    @type emr_iam_role: str
    @type log_bucket_name: str
    @rtype emr_cluster_id: str
    """
    emr_client = emr_client
    cluster_id = emr_client.run_job_flow(
        Name='spark-emr-cluster',
        #LogUri='s3://aws-logs-766155929240-us-west-2/elasticmapreduce/',
        LogUri=os.path.join('s3://', log_bucket_name, 'elasticmapreduce/'),
        ReleaseLabel='emr-5.29.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Core nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 4,
                }
            ],\
            # kill cluster after completing all steps
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', 's3://' + code_bucket_name, '/home/hadoop/', '--recursive']
                }
            },
            {
                'Name': 'setup - install libraries',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['sudo','pip','install','configparser']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/etl.py']
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole=emr_iam_role
    )
    emr_cluster_id = cluster_id['JobFlowId']
    
    print("EMR Cluster Created \n JobFlowId: {}".format(emr_cluster_id))
    
    return emr_cluster_id

def wait_till_cluster_deleted(emr_client, cluster_id):
    """
    Wait till EMR cluster is deleted.
    
    @type emr_client: boto3 client
    @type cluster_identifier: str
    @rtype: None
    """
    cluster_not_deleted = True
    
    while cluster_not_deleted:
        cluster_state = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']
        if cluster_state == "TERMINATED":
            cluster_not_deleted = False
        time.sleep(1)
        
    print("Cluster is deleted")
    
    return None

def delete_role_arn(iam_client, role_arn):
    """
    Detach and delete the roleARN.
    
    @type config_dict: dict
    @rtype: None
    """
    role_arn = role_arn
    
    iam_client.detach_role_policy(RoleName=role_arn,\
                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
    iam_client.detach_role_policy(RoleName=role_arn,\
                                  PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole")
    iam_client.delete_role(RoleName=role_arn)
    
    print("RoleArn is deleted")
    
    return None


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    iam_client = create_client("iam",\
                               region="us-west-2",\
                               access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],\
                               secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])
    s3_client = create_client("s3",\
                              region="us-west-2",\
                              access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],\
                              secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])
    emr_client = create_client("emr",\
                               region="us-west-2",\
                               access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],\
                               secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])
    
    # create s3 bucket for etl.py and dl.cfg
    try:
        create_s3_buckets(bucket_name=config['S3']['CODE_BUCKET_NAME'],\
                          s3_client=s3_client)
    except Exception as e:
        print(e)
    
    # create s3 bucket for ETL output tables
    try:
        create_s3_buckets(bucket_name=config['S3']['OUTPUT_BUCKET_NAME'],\
                          s3_client=s3_client)
    except Exception as e:
        print(e)
        
    # create s3 bucket for EMR Logs
    try:
        create_s3_buckets(bucket_name=config['S3']['EMR_LOG_BUCKET_NAME'],\
                          s3_client=s3_client)
    except Exception as e:
        print(e)
    
    # upload etl.py to s3 bucket
    try:
        upload_code(file_name="etl.py",\
                    bucket_name=config['S3']['CODE_BUCKET_NAME'],\
                    s3_client=s3_client)
        # create folder for storing log files
        s3_client.put_object(Bucket=config['S3']['CODE_BUCKET_NAME'], Key=('elasticmapreduce/'))
    except Exception as e:
        print(e)
    
    # upload dl.cfg to s3 bucket
    try:
        upload_code(file_name="dl.cfg",\
                    bucket_name=config['S3']['CODE_BUCKET_NAME'],\
                    s3_client=s3_client)
    except Exception as e:
        print(e)
    
    
    role_arn = create_iam_role(iam_client=iam_client,\
                               iam_role_name='MyEMRRole')
    
    # create emr cluster and run job
    try:
        emr_cluster_id = create_emr_cluster(code_bucket_name=config['S3']['CODE_BUCKET_NAME'],\
                                            emr_client=emr_client,\
                                            emr_iam_role='MyEMRRole',\
                                            log_bucket_name=config['S3']['EMR_LOG_BUCKET_NAME']
                                           )
    except Exception as e:
        print(e)
    
    wait_till_cluster_deleted(emr_client=emr_client,\
                              cluster_id=emr_cluster_id)
    
    
    delete_role_arn(iam_client=iam_client,\
                    role_arn="MyEMRRole")
    
if __name__ == '__main__':
    main()