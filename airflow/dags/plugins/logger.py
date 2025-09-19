import logging
import threading

class Logger:
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logging.basicConfig(
                        level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s"
                    )
                    cls._instance = logging.getLogger("AirflowLogger")
        return cls._instance
