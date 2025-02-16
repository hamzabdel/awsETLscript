import pandas as pd
import os
from botocore.exceptions import ClientError
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION
import logging
import boto3

logging.basicConfig(level=logging.INFO)

s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = AWS_REGION
)

def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        logging.info(f"Starting upload of {file_path} to {bucket_name}/{s3_key}")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Successfully uploaded {file_path} to {bucket_name}/{s3_key}")
        return True
    except ClientError as e:
        logging.error(f"Upload failed: {e}")
        return False

file_path = 'people-10000.csv' #put your own info here
bucket_name = 's3etlbucket'
s3_key = 'raw/people-10000.csv'

upload_to_s3(file_path, bucket_name, s3_key)

