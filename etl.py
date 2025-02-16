import boto3
import time
from botocore.exceptions import ClientError
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION
import logging

logging.basicConfig(level=logging.INFO)

glue_client = boto3.client('glue',
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = AWS_REGION
)

def run_etl_pipeline():
    logging.info("starting etl")

    try:
        logging.info("Starting crawler...")
        glue_client.start_crawler(Name='s3-etl-crawler')

        while True:
            crawler_state = glue_client.get_crawler(Name='s3-etl-crawler')['Crawler']['State']
            if crawler_state == 'READY':
                logging.info("Crawler completed!")
                break
            time.sleep(30)
        
        logging.info("Starting ETL job...")
        response = glue_client.start_job_run(
            JobName = 's3_to_aurora_etl'
        )

        job_run_id = response['JobRunId']
        while True:
            status = check_job_status('s3_to_aurora_etl', job_run_id)
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                logging.info(f"ETL job finished with status: {status}")
                break
            time.sleep(60)

    except ClientError as e:
        logging.error(f"ETL pipeline failed: {e}")
        return False
    
    return True

def check_job_status(job_name, run_id):
    try:
        response = glue_client.get_job_run(
            JobName = job_name, 
            RunId = run_id
        )
        return response['JobRun']['JobRunState']
    except ClientError as e:
        logging.error(f"Error checking job status {e}")
        return 'FAILED'
    
if __name__ == "__main__":
    run_etl_pipeline()