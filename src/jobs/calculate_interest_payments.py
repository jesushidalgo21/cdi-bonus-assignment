from src.interest_calculation import InterestCalculator
from src.utils import parse_args
from src.logger import Logger
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()

def main():
    args = parse_args("Calcula pagos de intereses con base en historial de wallet y tasas CDI.")
    start_date = args.start
    end_date = args.end

    spark_manager = SparkSessionManager()
    logger.info(f"🚀 Iniciando calculo de interes entre {start_date} y {end_date}")

    try:
        calculator = InterestCalculator()
        calculator.calculate_and_save_interest(start_date, end_date)
    except Exception as e:
        logger.error(f"❌ Error en el proceso: {e}", exc_info=True)
        raise
    finally:
        spark_manager.stop_spark_session()

if __name__ == "__main__":
    main()
