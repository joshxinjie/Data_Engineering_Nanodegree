import time

import json
import boto3
import pandas as pd
import configparser
import psycopg2

def read_dwh_config(config_file_path):
    """
    Read in the settings in the configuration file.
    
    @type config_file_path: str
    @rtype config_dict: dict
    """
    
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))
    
    config_dict = {"KEY": config.get('AWS','KEY'),\
                   "SECRET": config.get('AWS','SECRET'),\
                   "DWH_CLUSTER_TYPE": config.get("DWH","DWH_CLUSTER_TYPE"),\
                   "DWH_NUM_NODES": config.get("DWH","DWH_NUM_NODES"),\
                   "DWH_NODE_TYPE": config.get("DWH","DWH_NODE_TYPE"),\
                   "DWH_CLUSTER_IDENTIFIER": config.get("DWH","DWH_CLUSTER_IDENTIFIER"),\
                   "DWH_DB": config.get("DWH","DWH_DB"),\
                   "DWH_DB_USER": config.get("DWH","DWH_DB_USER"),\
                   "DWH_DB_PASSWORD": config.get("DWH","DWH_DB_PASSWORD"),\
                   "DWH_PORT": config.get("DWH","DWH_PORT"),\
                   "DWH_IAM_ROLE_NAME": config.get("DWH", "DWH_IAM_ROLE_NAME")
                  }
    
    return config_dict

def create_clients(region_name, aws_access_key_id, aws_secret_access_key):
    """
    Create clients for the EC2, s3, IAM and Redshift resources.
    
    @type region_name: str
    @type aws_access_key_id: str
    @type aws_secret_access_key: str
    @rtype clients_dict: dict
    """
    ec2 = boto3.resource('ec2',\
                     region_name=region_name,\
                     aws_access_key_id=aws_access_key_id,\
                     aws_secret_access_key=aws_secret_access_key
                    )

    s3 = boto3.resource('s3',\
                        region_name=region_name,\
                        aws_access_key_id=aws_access_key_id,\
                        aws_secret_access_key=aws_secret_access_key
                       )

    iam = boto3.client('iam',\
                       region_name=region_name,\
                       aws_access_key_id=aws_access_key_id,\
                       aws_secret_access_key=aws_secret_access_key
                      )

    redshift = boto3.client('redshift',\
                            region_name=region_name,\
                            aws_access_key_id=aws_access_key_id,\
                            aws_secret_access_key=aws_secret_access_key
                           )
    
    clients_dict = {"ec2": ec2, "s3": s3, "iam": iam, "redshift": redshift}
    
    return clients_dict

def create_iam_role(iam, iam_role_name):
    """
    Create an IAM Role that allows Redshift to have ReadOnly access to the S3 bucket
    
    @type iam: boto3 client
    @type iam_role_name: str
    @rtype role_arn: str
    """
    try:
        print('Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',\
            RoleName=iam_role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal':{'Service': 'redshift.amazonaws.com'}
                               }],
                 'Version': '2012-10-17'
                }
            )
        )
    except Exception as e:
        print(e)

    print('Attaching Policy')
    iam.attach_role_policy(RoleName=iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print('Getting the IAM role ARN')
    role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    
    print("IAM Role ARN created: "+str(role_arn))

    return role_arn

def create_redshift_cluster(redshift, config_dict, role_arn):
    """
    Create the Reshift cluster.
    
    @type redshift: boto3 client
    @type config_dict: dict
    @type role_arn: str
    @rtype: None
    """
    response = redshift.create_cluster(        
        # Add parameters for hardware
        ClusterType=config_dict["DWH_CLUSTER_TYPE"],
        NodeType=config_dict["DWH_NODE_TYPE"],
        NumberOfNodes=int(config_dict["DWH_NUM_NODES"]),

        # Add parameters for identifiers & credentials
        DBName=config_dict["DWH_DB"],
        ClusterIdentifier=config_dict["DWH_CLUSTER_IDENTIFIER"],
        MasterUsername=config_dict["DWH_DB_USER"],
        MasterUserPassword=config_dict["DWH_DB_PASSWORD"],
        
        # Add parameter for role (to allow s3 access)
        IamRoles=[role_arn]
    )
    return None

def wait_till_cluster_available(cluster_identifier, redshift):
    """
    Wait till the Redshift cluster's status is available, then return the cluster properties.
    
    @type cluster_identifier: str
    @type redshift: boto3 client
    @rtype cluster_properties: dict
    """
    cluster_loading = True
    
    while cluster_loading:
        cluster_properties = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        cluster_status = cluster_properties["ClusterStatus"]
        if cluster_status == "available":
            cluster_loading = False
        time.sleep(1)
        
    print("Cluster is available...")
    print("Cluster Endpoint: "+str(cluster_properties["Endpoint"]["Address"]))
    
    return cluster_properties

def write_etl_config(config_file_path, cluster_properties, config_dict, role_arn):
    """
    Updates the configuration file with the newly created Redshift cluster endpoint
    and the role ARN.
    
    @type config_file_path: str
    @type cluster_properties: dict
    @type role_arn: str
    @rtype: None
    """
    config = configparser.ConfigParser()
    
    config.read(config_file_path)
    
    config_file = open(config_file_path, "w")
    
    config.set("CLUSTER", "HOST", cluster_properties["Endpoint"]["Address"])
    config.set("CLUSTER", "DB_NAME", config_dict["DWH_DB"])
    config.set("CLUSTER", "DB_USER", config_dict["DWH_DB_USER"])
    config.set("CLUSTER", "DB_PASSWORD", config_dict["DWH_DB_PASSWORD"])
    config.set("CLUSTER", "DB_PORT", config_dict["DWH_PORT"])
    config.set("IAM_ROLE", "ARN", role_arn)
    
    config.write(config_file)
    config_file.close()
    
    return None

def open_tcp_port(ec2, cluster_properties, dwh_port):
    """
    Open an incoming TCP port to access the cluster endpoint. Allows access to the
    Redshift cluster from outside.
    
    @type ec2: boto3 resource
    @type cluster_properties: dict
    @type dwh_port: str
    @rtype: None
    """
    vpc = ec2.Vpc(id=cluster_properties['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    
    defaultSg.authorize_ingress(
        GroupName='default',
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(dwh_port),
        ToPort=int(dwh_port)
    )
    
    return None

def check_connection(config_file_path):
    """
    Check if there is a connection to the Redshift cluster.
    
    @type config_file_path: str
    @rtype: None
    """
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Database connected')
    
    return None

if __name__ == "__main__":
    config_file_path = "dwh.cfg"
    config_dict = read_dwh_config(config_file_path)
    
    clients_dict = create_clients(region_name="ap-southeast-1",\
                                  aws_access_key_id=config_dict["KEY"],\
                                  aws_secret_access_key=config_dict["SECRET"])
    
    role_arn = create_iam_role(iam=clients_dict["iam"],\
                               iam_role_name=config_dict["DWH_IAM_ROLE_NAME"])
        
    try:
        create_redshift_cluster(redshift=clients_dict["redshift"],\
                                config_dict=config_dict,\
                                role_arn=role_arn)
    except Exception as e:
        print(e)
        
    cluster_properties = wait_till_cluster_available(cluster_identifier=config_dict["DWH_CLUSTER_IDENTIFIER"],\
                                                     redshift=clients_dict["redshift"]
                                                    )
    
    write_etl_config(config_file_path, cluster_properties, config_dict, role_arn)
    
    try:
        open_tcp_port(ec2=clients_dict["ec2"],\
                      cluster_properties=cluster_properties,\
                      dwh_port=config_dict["DWH_PORT"])
    except Exception as e:
        print(e)
    
    # check connection to the Redshift cluster
    check_connection(config_file_path)