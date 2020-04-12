import time

import boto3
import configparser

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

def delete_cluster(redshift, cluster_identifier):
    """
    Delete the Redshift cluster.
    
    @type redshift: boto3 client
    @type cluster_identifier: str
    @rtype: None
    
    cluster_identifier=DWH_CLUSTER_IDENTIFIER
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)
    
    return None

def wait_till_cluster_deleted(redshift, cluster_identifier):
    """
    Wait till Redshift cluster is deleted.
    
    @type redshift: boto3 client
    @type cluster_identifier: str
    @rtype: None
    
    cluster_identifier=config_dict["DWH_CLUSTER_IDENTIFIER"]
    """
    cluster_not_deleted = True
    
    while cluster_not_deleted:
        try:
            cluster_properties = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        except Exception as e:
            print(e)
            cluster_not_deleted = False
        time.sleep(1)
        
    print("Cluster is deleted...")
    
    return None

def delete_role_arn(iam, config_dict):
    """
    Detach and delete the roleARN.
    
    @type config_dict: dict
    @rtype: None
    """
    role_arn = config_dict["DWH_IAM_ROLE_NAME"]
    
    iam.detach_role_policy(RoleName=role_arn,\
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=role_arn)
    
    print("RoleArn is deleted...")
    
    return None

if __name__ == "__main__":
    config_file_path = "dwh.cfg"
    config_dict = read_dwh_config(config_file_path)
    
    print(config_dict)
    
    clients_dict = create_clients(region_name="ap-southeast-1",\
                                  aws_access_key_id=config_dict["KEY"],\
                                  aws_secret_access_key=config_dict["SECRET"])
    
    delete_cluster(redshift=clients_dict["redshift"],\
                   cluster_identifier=config_dict["DWH_CLUSTER_IDENTIFIER"])
    
    wait_till_cluster_deleted(redshift=clients_dict["redshift"],\
                              cluster_identifier=config_dict["DWH_CLUSTER_IDENTIFIER"])
    
    delete_role_arn(iam=clients_dict["iam"],\
                    config_dict=config_dict)