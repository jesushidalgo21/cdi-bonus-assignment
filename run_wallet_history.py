import argparse
import sys
from pyspark.sql import SparkSession
from src.wallet_history_builder import generate_wallet_history
from src.utils import load_config
import logging

def setup_logging():
    """Configura logging para mostrar mensajes en consola."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdc-path", help="Ruta a los archivos CDC", default=None)
    args = parser.parse_args()

    config = load_config()

    spark = SparkSession.builder \
        .appName("cdi-bonus") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/app/log4j.properties") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        logging.info("Iniciando procesamiento de datos CDC...")
        generate_wallet_history(
            cdc_path=args.cdc_path or config["paths"]["cdc"],
            spark=spark
        )
        logging.info("Proceso completado exitosamente!")
    except ValueError as e:
        logging.error(f"Error de validación:\n{str(e)}")
        spark.stop()
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Error inesperado: {str(e)}", exc_info=True)
        sys.exit(2)

if __name__ == "__main__":
    main()