from pyspark.sql import SparkSession
from typing import Optional
import atexit

class SparkSessionManager:
    _instance: Optional['SparkSessionManager'] = None
    _spark_session: Optional[SparkSession] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
            cls._instance._initialize_spark_session()
            atexit.register(cls._cleanup)
        return cls._instance

    @classmethod
    def _cleanup(cls):
        """Método de clase para limpieza con atexit"""
        if cls._instance:
            cls._instance.stop_spark_session()

    def _initialize_spark_session(self):
        """Configuración centralizada de Spark"""
        if self._spark_session is None:
            self._spark_session = SparkSession.builder \
                .appName("DefaultAppName") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/app/log4j.properties") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "8g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            self._spark_session.sparkContext.setLogLevel("ERROR")

    def get_spark_session(self, app_name: Optional[str] = None) -> SparkSession:
        """Obtiene la SparkSession, opcionalmente actualizando el appName"""
        if not self._spark_session:
            self._initialize_spark_session()
            
        if app_name:
            self._spark_session.conf.set("spark.app.name", app_name)
        return self._spark_session

    def stop_spark_session(self):
        """Detiene la SparkSession de manera segura"""
        if self._spark_session:
            try:
                # Verificación más segura del estado de Spark
                if not self._spark_session._sc._jsc.sc().isStopped():
                    self._spark_session.stop()
            except Exception as e:
                print(f"Warning: Error al detener SparkSession: {str(e)}")
            finally:
                self._spark_session = None
                SparkSessionManager._instance = None

    def __del__(self):
        """Destructor como último recurso"""
        self.stop_spark_session()