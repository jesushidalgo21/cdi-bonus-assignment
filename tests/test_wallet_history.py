import pytest
from pyspark.sql import SparkSession
from src.wallet_history_builder import WalletHistoryGenerator
from src.schemas import cdc_data_schema
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("WalletHistoryTest") \
        .getOrCreate()

@pytest.fixture
def sample_cdc_data(spark):
    data = [
        ("acc1", "user1", "DEPOSIT", 100, datetime.fromisoformat("2024-01-01T10:00:00")),
        ("acc1", "user1", "WITHDRAWAL", 50, datetime.fromisoformat("2024-01-01T12:00:00")),
        ("acc1", "user1", "TRANSFER_OUT", 25, datetime.fromisoformat("2024-01-02T10:00:00")),
        ("acc1", "user1", "DEPOSIT", 200, datetime.fromisoformat("2024-01-02T15:00:00")),
    ]
    return spark.createDataFrame(data, cdc_data_schema)

@pytest.fixture
def empty_cdc_data(spark):
    return spark.createDataFrame([], cdc_data_schema)

def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def test_wallet_history_happy_path(tmp_path, spark, sample_cdc_data):
    input_path = str(tmp_path / "input")
    write_parquet(sample_cdc_data, input_path)

    generator = WalletHistoryGenerator()

    with patch("src.wallet_history_builder.get_postgres_connection") as mock_conn, \
         patch("pyspark.sql.DataFrame.write") as mock_write:

        mock_conn.return_value = ("mock_url", {"user": "x", "password": "y"})
        mock_write.jdbc = MagicMock()

        generator.load_wallet_history(input_path, "2024-01-01", "2024-01-02")
        assert mock_write.jdbc.called

def test_wallet_history_validation_fails(tmp_path, spark, sample_cdc_data):
    input_path = str(tmp_path / "input")
    write_parquet(sample_cdc_data, input_path)

    generator = WalletHistoryGenerator()

    with patch("src.wallet_history_builder.CDCDataValidator.validate", return_value=(False, ["Error de validación"])):
        with pytest.raises(ValueError, match="Error de validación"):
            generator.load_wallet_history(input_path, "2024-01-01", "2024-01-02")

def test_wallet_history_empty_data(tmp_path, spark, empty_cdc_data):
    input_path = str(tmp_path / "empty")
    write_parquet(empty_cdc_data, input_path)

    generator = WalletHistoryGenerator()

    with patch("pyspark.sql.DataFrame.write") as mock_write:
        generator.load_wallet_history(input_path, "2024-01-01", "2024-01-02")
        assert not mock_write.method_calls, "No debería intentar escribir si no hay datos"

def test_wallet_history_negative_balance(tmp_path, spark):
    data = [
        ("acc1", "user1", "WITHDRAWAL", 300, datetime.fromisoformat("2024-01-01T10:00:00")),
        ("acc1", "user1", "DEPOSIT", 100, datetime.fromisoformat("2024-01-01T12:00:00")),
    ]
    df = spark.createDataFrame(data, cdc_data_schema)
    input_path = str(tmp_path / "neg_balance")
    write_parquet(df, input_path)

    generator = WalletHistoryGenerator()

    with patch("pyspark.sql.DataFrame.write") as mock_write, \
         patch("src.wallet_history_builder.get_postgres_connection") as mock_conn:
        mock_conn.return_value = ("mock_url", {"user": "x", "password": "y"})
        mock_write.jdbc = MagicMock()

        generator.load_wallet_history(input_path, "2024-01-01", "2024-01-01")
        assert mock_write.jdbc.called

def test_wallet_history_invalid_schema(tmp_path, spark):
    invalid_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ])
    data = [
        ("acc1", "user1", "DEPOSIT", datetime.fromisoformat("2024-01-01T10:00:00")),
    ]
    df = spark.createDataFrame(data, invalid_schema)
    input_path = str(tmp_path / "bad_schema")
    write_parquet(df, input_path)

    generator = WalletHistoryGenerator()

    with pytest.raises(Exception):
        generator.load_wallet_history(input_path, "2024-01-01", "2024-01-01")
