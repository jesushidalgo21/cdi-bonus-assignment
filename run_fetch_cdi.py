import argparse
from pyspark.sql import SparkSession
from src.fetcher import fetch_cdi_rates, save_partitioned_parquet
from src.utils import load_config

def main(spark: SparkSession, start_date: str, end_date: str, output_path: str):
    print(f"📅 Descargando CDI desde {start_date} hasta {end_date}")
    df = fetch_cdi_rates(spark, start_date, end_date)
    save_partitioned_parquet(df, output_path)
    print(f"✅ CDI guardado en: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch CDI rates from BCB API.")
    parser.add_argument("--partition_date", type=str, help="Fecha individual a procesar (YYYY-MM-DD).")
    parser.add_argument("--start_date", type=str, help="Fecha de inicio para reproceso (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="Fecha de fin para reproceso (YYYY-MM-DD).")

    args = parser.parse_args()

    # Cargar configuración
    config = load_config()
    output_path = config["paths"]["cdi_rates"]

    if args.partition_date:
        start_date = end_date = args.partition_date
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        raise ValueError("Debes especificar --partition_date o ambos --start_date y --end_date")

    # Crear sesión de Spark
    spark = SparkSession.builder.appName("FetchCDI").getOrCreate()

    # Llamar a la función main con los argumentos procesados
    main(spark, start_date, end_date, output_path)

    # Detener la sesión de Spark
    spark.stop()