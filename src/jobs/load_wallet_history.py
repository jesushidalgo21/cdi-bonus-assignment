
from src.utils import parse_args, load_config
from src.logger import Logger
from src.wallet_history_builder import WalletHistoryGenerator
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()

def main():
    args = parse_args("Carga de historial de billeteras")
    start_date = args.start
    end_date = args.end

    spark_manager = SparkSessionManager()
    logger.info(f"🚀 Iniciando carga de historial entre {start_date} y {end_date}")
    config = load_config()

    try:
        wallet_history = WalletHistoryGenerator()
        wallet_history.load_wallet_history(config["paths"]["cdc"], start_date, end_date)
    except Exception as e:
        logger.error(f"❌ Error en el proceso: {e}", exc_info=True)
        raise
    finally:
        spark_manager.stop_spark_session()

if __name__ == "__main__":
    main()
