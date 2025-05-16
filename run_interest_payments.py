import argparse
from pyspark.sql import SparkSession
from src.interest_calculation import calculate_interest
from src.utils import load_config, get_postgres_connection
from src.schemas import wallet_history_schema, cdi_rates_schema

def load_filtered_data(spark, config, start_date, end_date=None):
    """
    Carga y filtra los datos de wallet_history desde PostgreSQL y cdi_rates desde Parquet.
    """
    wallet_history = load_wallet_history_from_pg(spark, config)
    wallet_history.show(20, False)
    cdi_rates = load_cdi_rates_from_parquet(spark, config)
    cdi_rates.show(20, False)

    wallet_history = filter_by_partition_date(wallet_history, start_date, end_date)
    cdi_rates = filter_by_partition_date(cdi_rates, start_date, end_date)

    return wallet_history, cdi_rates

def load_wallet_history_from_pg(spark, config):
    pg_conf = config["postgres"]
    query = f"(SELECT * FROM {pg_conf['schema']}.{pg_conf['wallet_history_table']}) AS wallet_history"
    
    jdbc_url, connection_props = get_postgres_connection(pg_conf)
    
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("user", connection_props["user"]) \
        .option("password", connection_props["password"]) \
        .option("driver", connection_props["driver"]) \
        .load()

def load_cdi_rates_from_parquet(spark, config):
    path = config["paths"]["cdi_rates"]
    return spark.read.schema(cdi_rates_schema).parquet(path)

def filter_by_partition_date(df, start_date, end_date=None):
    if end_date is None:
        return df.filter(df.partition_date == start_date)
    else:
        return df.filter(
            (df.partition_date >= start_date) & 
            (df.partition_date <= end_date)
        )

def main(start_date: str, end_date: str = None):
    config = load_config()
    spark = SparkSession.builder.appName("InterestPayments").getOrCreate()
    
    wallet_df, cdi_df = load_filtered_data(spark, config, start_date, end_date)
    interest_df = calculate_interest(wallet_df, cdi_df)
    interest_df.show(20, False)

    pg_conf = config["postgres"]
    jdbc_url, connection_props = get_postgres_connection(pg_conf)
    table = f"{pg_conf['schema']}.{pg_conf['interest_payments_table']}"

    interest_df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode="overwrite",
        properties=connection_props
    )

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end", help="Fecha de fin (YYYY-MM-DD), opcional si es ejecución diaria")
    args = parser.parse_args()

    main(args.start, args.end)