from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_session import SparkSessionSingleton
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_spark_session():
    logger.info("Starting Spark session test...")
    spark = SparkSessionSingleton.get_instance()
    logger.info("Spark session created successfully.")
    data = [("Hello, world!",)]
    columns = ["test"]
    df = spark.createDataFrame(data, columns)
    df.show()
    logger.info("Spark session is working correctly.")


with DAG(
    dag_id="test_spark_session",
    start_date=datetime(2025, 1, 1),  # fecha pasada, así no se ejecuta automáticamente
    schedule_interval=None,           # sin schedule, solo manual
    catchup=False,                   # evita que Airflow intente "ponerse al día"
    tags=["test", "spark"]
) as dag:

    test_task = PythonOperator(
        task_id="test_spark_session",
        python_callable=test_spark_session
    )