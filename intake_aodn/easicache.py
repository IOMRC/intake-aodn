import boto3
from datetime import datetime as dt
import glob
import os
import intake
import logging
from botocore.exceptions import ClientError
scratch_bucket = "easihub-csiro-user-scratch"

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # boto3
    if 's3' not in locals():
        s3 = boto3.client('s3')
        
    # Upload the file
    try:
        s3.upload_file(file_name, bucket, object_name)  # Config=config
    except (ClientError, FileNotFoundError) as e:
        logging.error(e)
        return False
    return True

def set_easi():
    s3 = boto3.client('s3')
    userid = boto3.client('sts').get_caller_identity()['UserId']
    files= glob.glob(f'{os.path.dirname(__file__)}/catalogs/*.*')
    for f in files:
        print (f,os.path.basename(f),f's3://{scratch_bucket}/{userid}/{os.path.basename(f)}')
        upload_file(f, scratch_bucket, f'{userid}/{os.path.basename(f)}')
    return intake.open_catalog(f's3://{scratch_bucket}/{userid}/main.yaml')
    
