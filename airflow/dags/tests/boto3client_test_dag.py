from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from boto3_client import Boto3Client
from logger import Logger

logger = Logger.get_instance()
# testeo de boto3 para minio

def test_boto3_client_conn():
    try:
        logger.info("Testing Boto3 client connection...")
        s3 = Boto3Client.get_instance()
        logger.info("Boto3 client connection successful.")    
    except Exception as e:
        logger.error(f"Boto3 client connection failed: {e}")
        raise

def test_boto3_buckets():
    logger.info("Starting Boto3 client test...")
    s3 = Boto3Client.get_instance()
    logger.info("Boto3 client created successfully.")

    bucket_names = ["raw-test", "trusted-test", "refined-test"]
    for bucket in bucket_names:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' created.")

    buckets = s3.list_buckets()
    logger.info(f"Buckets after creation: {buckets}")

    for bucket in bucket_names:
        s3.delete_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' deleted.")

    buckets_after_delete = s3.list_buckets()
    logger.info(f"Buckets after deletion: {buckets_after_delete}")

with DAG(
    dag_id="test_boto3_client",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,           
    catchup=False,                   
    tags=["test", "boto3"]
) as dag:

    test_conn_task = PythonOperator(
        task_id="test_boto3_client_conn",
        python_callable=test_boto3_client_conn
    )

    test_buckets_task = PythonOperator(
        task_id="test_boto3_buckets",
        python_callable=test_boto3_buckets
    )

    test_conn_task >> test_buckets_task