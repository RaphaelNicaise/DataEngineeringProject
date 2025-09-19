from pyspark.sql import SparkSession
import threading

class SparkSessionSingleton:
    _instance = None
    _lock = threading.Lock()  

    @classmethod
    def get_instance(cls, app_name="AirflowApp", master="local[*]"):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = (
                        SparkSession.builder
                        .appName(app_name)
                        .master(master)
                        .config("spark.sql.shuffle.partitions", "4")  
                        .getOrCreate()
                    )
        return cls._instance