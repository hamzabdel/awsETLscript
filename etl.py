import boto3
import time
import boto3.s3
from botocore.exceptions import ClientError
import logging
from upload import upload_to_s3
from config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_REGION
)

class DataWarehouseETL:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = AWS_REGION
        )
        self.redshift_client = boto3.client( # accessibility eventually
            'redshift',
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = AWS_REGION
        )
        self.glue_client = boto3.client(
            'glue',
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = AWS_REGION
        )

        self.athena_client = boto3.client(
            'athena',
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = AWS_REGION
        )

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def run_crawler(self, crawler_name):
        try:
            self.logger.info(f"Starting crawler: {crawler_name}")
            
            self.glue_client.start_crawler(Name=crawler_name)

            while True:
                crawler_state = self.glue_client.get_crawler(Name=crawler_name)['Crawler']['State']

                if crawler_state == 'READY':
                    self.logger.info(f"Crawler {crawler_name} completed!")
                elif crawler_state in ['STOPPING', 'STOPPED']:
                    self.logger.warning(f"Crawler {crawler_name} stopped.")
                    break
        
                time.sleep(30) #Checks again after 30 seconds

            return True
        except ClientError as e:
            self.logger.error(f"Crawler error: {e}")
            return False
    
    def run_etl_job(self, job_name):
        try:
            self.logger.info(f"Starting ETL job: {job_name}")

            response = self.glue_client.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']

            while True:
                status = self.check_job_status(job_name, job_run_id)

                if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                    self.logger.info(f"ETL job {job_name} finished with status: {status}")
                    return status == "SUCCEEDED"

                time.sleep(60) #tries again after 60 seconds

        except ClientError as e:
            self.logger.error("ETL job error: {e}")
            return False
    
    def check_job_status(self, job_name, run_id):
        try:
            response = self.glue_client.get_job_run(
                JobName = job_name,
                RunId = run_id
            )
            return response['JobRun']['JobRunState']
        except ClientError as e:
            self.logger.error(f"Error checking job status: {e}")
            return 'FAILED'
    
    def run_data_warehouse_pipeline(self, crawler_name='s3-etl-crawler', etl_job_name='covidETL'):
        try:
            crawler_success = self.run_crawler(crawler_name)
            if not crawler_success:
                raise Exception("Crawler failed")
            
            etl_success = self.run_etl_job(etl_job_name)
            if not etl_success:
                raise Exception("ETL Job Failed")
            
            self.logger.info("Data warehouse pipeline completed!")
        
        except Exception as e:
            self.logger.error(f"Data warehouse pipeline failed! {e}")
            raise
    
    def loadToAthena(self, bucket_name, file_path, table_name):
        database_name = "covid_database"
        s3_path = f"s3://covidbucketwarehouse/{file_path}"

        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
            `Province_State` STRING,
            `Country_Region` STRING,
            `Lat` DOUBLE,
            `Long` DOUBLE,
            `Date` STRING,
            `Confirmed` INT,
            `Deaths` INT,
            `Recovered` INT,
            `Active` INT,
            `WHO_Region` STRING
        )




if __name__ == "__main__":
    dw_etl = DataWarehouseETL()
    dw_etl.run_data_warehouse_pipeline()
