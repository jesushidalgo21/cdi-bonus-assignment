import pytest
from pyspark.sql import SparkSession
from src.wallet_history_builder import generate_wallet_history
from src.schemas import cdc_data_schema
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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

def test_generate_wallet_history_happy_path(tmp_path, spark, sample_cdc_data):
    input_path = str(tmp_path / "input")
    write_parquet(sample_cdc_data, input_path)

    with patch("src.wallet_history_builder.validate_cdc_data", return_value=(True, [])), \
         patch("pyspark.sql.DataFrame.write") as mock_write:

        mock_jdbc = MagicMock()
        mock_write.jdbc = mock_jdbc

        generate_wallet_history(input_path, spark)

        assert mock_write.method_calls, "No se llamó a ningún método de escritura"
        assert mock_write.jdbc.called, ".write.jdbc() no fue llamado"

def test_generate_wallet_history_validation_fails(tmp_path, spark, sample_cdc_data):
    input_path = str(tmp_path / "input")
    write_parquet(sample_cdc_data, input_path)

    with patch("src.wallet_history_builder.validate_cdc_data", return_value=(False, ["Error de validación"])):
        with pytest.raises(ValueError, match="Error de validación"):
            generate_wallet_history(input_path, spark)

def test_generate_wallet_history_empty_data(tmp_path, spark, empty_cdc_data):
    input_path = str(tmp_path / "empty")
    write_parquet(empty_cdc_data, input_path)

    with patch("src.wallet_history_builder.validate_cdc_data", return_value=(True, [])), \
         patch("pyspark.sql.DataFrame.write") as mock_write:

        generate_wallet_history(input_path, spark)
        assert not mock_write.method_calls, "No debería intentar escribir si no hay datos"

def test_generate_wallet_history_negative_balance(tmp_path, spark):
    data = [
        ("acc1", "user1", "WITHDRAWAL", 300, datetime.fromisoformat("2024-01-01T10:00:00")),
        ("acc1", "user1", "DEPOSIT", 100, datetime.fromisoformat("2024-01-01T12:00:00")),
    ]
    df = spark.createDataFrame(data, cdc_data_schema)
    input_path = str(tmp_path / "neg_balance")
    write_parquet(df, input_path)

    with patch("src.wallet_history_builder.validate_cdc_data", return_value=(True, [])), \
         patch("pyspark.sql.DataFrame.write") as mock_write:

        generate_wallet_history(input_path, spark)
        assert mock_write.jdbc.called

def test_generate_wallet_history_invalid_schema(tmp_path, spark):
    # Faltando columna "amount"
    invalid_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("event_time", StringType(), True),
    ])
    data = [
        ("acc1", "user1", "DEPOSIT", datetime.fromisoformat("2024-01-01T10:00:00")),
    ]
    df = spark.createDataFrame(data, invalid_schema)
    input_path = str(tmp_path / "bad_schema")
    write_parquet(df, input_path)

    with pytest.raises(Exception):
        generate_wallet_history(input_path, spark)
