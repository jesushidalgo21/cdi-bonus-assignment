import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import date
from unittest.mock import patch
from src.interest_calculation import InterestCalculator

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("InterestCalcTest").getOrCreate()

@pytest.fixture
def wallet_history_df(spark):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("immovable_balance", DoubleType(), True),
        StructField("partition_date", DateType(), True),
    ])
    data = [
        ("user1", "acc1", 150.0, date(2024, 5, 1)),
        ("user2", "acc2", 80.0, date(2024, 5, 1)),
        ("user3", "acc3", 200.0, date(2024, 5, 1)),
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def cdi_rates_df(spark):
    schema = StructType([
        StructField("cdi_rate", DoubleType(), True),
        StructField("partition_date", DateType(), True),
    ])
    data = [
        (0.1, date(2024, 5, 1)),
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def dummy_config():
    return {
        "postgres": {
            "schema": "wallet",
            "interest_payments_table": "interest_payments",
            "interest_audit_log_table": "interest_audit_log",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver"
        }
    }

def test_calculate_interest_with_audit(wallet_history_df, cdi_rates_df, dummy_config):
    calc = InterestCalculator()
    with patch("src.audit_logger.InterestAuditLogger.save_to_postgres_df") as mock_save:
        result_df = calc._calculate_interest_with_audit(wallet_history_df, cdi_rates_df, dummy_config, min_balance=100.0, decimal_places=2)

        result = result_df.select("user_id", "account_id", "eligible_balance", "interest_amount").orderBy("user_id").collect()
        assert len(result) == 3
        # user1 y user3 califican, user2 no
        assert result[0]["user_id"] == "user1"
        assert result[0]["interest_amount"] > 0
        assert result[1]["user_id"] == "user2"
        assert result[1]["interest_amount"] == 0
        assert result[2]["user_id"] == "user3"
        assert result[2]["interest_amount"] > 0