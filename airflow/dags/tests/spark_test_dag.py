from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from spark_session import SparkSessionSingleton

def test_spark_session():
    spark = SparkSessionSingleton.get_instance()
    data = [("Hello, world!",)]
    columns = ["test"]
    df = spark.createDataFrame(data, columns)
    df.show()
    print("Spark session is working correctly.")


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