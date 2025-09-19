import boto3
from botocore.client import Config
from dotenv import load_dotenv
import os
import threading

load_dotenv()

class Boto3Client:
    """Para conectar a MinIO usando boto3.

    Returns:
        instance de boto3 client.
    """
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = boto3.client(
                's3',
                endpoint_url='http://minio:9000',  # URL de MinIO
                aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
                aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
        return cls._instance
