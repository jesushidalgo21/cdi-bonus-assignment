from src.fetcher import CDIDataFetcher
from src.utils import load_config, parse_args
from src.logger import Logger
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()

def main():
    args = parse_args("Descarga tasas CDI desde el API del BCB.")
    start_date = args.start
    end_date = args.end
 
    spark_manager = SparkSessionManager()
    logger.info(f"📅 Iniciando descarga de CDI desde {start_date} hasta {end_date}")
    config = load_config()

    try:
        fetcher = CDIDataFetcher()
        fetcher.fetch_and_save_cdi_rates(config["paths"]["cdi_rates"], start_date, end_date)
    except Exception as e:
        logger.error(f"❌ Error en el proceso: {e}", exc_info=True)
        raise
    finally:
        spark_manager.stop_spark_session()

if __name__ == "__main__":
    main()
