from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_session import SparkSessionClient
from logger import Logger

logger = Logger.get_instance()

def test_spark_session():
    logger.info("Starting Spark session test...")
    spark = SparkSessionClient.get_instance()
    logger.info("Spark session created successfully.")
    data = [("Hello, world!",)]
    columns = ["test"]
    df = spark.createDataFrame(data, columns)
    df.show()
    logger.info("Spark session is working correctly.")


with DAG(
    dag_id="test_spark_session",
    start_date=datetime(2025, 1, 1), 
    schedule_interval=None,           
    catchup=False,                  
    tags=["test", "spark"]
) as dag:

    test_task = PythonOperator(
        task_id="test_spark_session",
        python_callable=test_spark_session
    )