import pytest
from pyspark.sql import SparkSession, Row
from src.validation import CDCDataValidator

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("ValidationTests") \
        .getOrCreate()

def test_valid_data_passes_validation(spark):
    data = [
        Row(transaction_type="DEPOSIT", amount=100.0, account_id="A1", event_time="2024-01-01T10:00:00"),
        Row(transaction_type="WITHDRAWAL", amount=50.0, account_id="A1", event_time="2024-01-01T12:00:00"),
        Row(transaction_type="WALLET_CREATED", amount=0.0, account_id="A2", event_time="2024-01-02T09:00:00")
    ]
    df = spark.createDataFrame(data)
    validator = CDCDataValidator(df)
    is_valid, errors = validator.validate()

    assert is_valid is True
    assert errors == []

def test_wallet_created_with_nonzero_amount_fails(spark):
    data = [
        Row(transaction_type="WALLET_CREATED", amount=5.0, account_id="A1", event_time="2024-01-01T08:00:00")
    ]
    df = spark.createDataFrame(data)
    validator = CDCDataValidator(df)
    is_valid, errors = validator.validate()

    assert is_valid is False
    assert any("cuentas creadas con monto distinto de cero" in e for e in errors)

def test_deposit_with_negative_amount_fails(spark):
    data = [
        Row(transaction_type="DEPOSIT", amount=-100.0, account_id="A1", event_time="2024-01-01T08:00:00")
    ]
    df = spark.createDataFrame(data)
    validator = CDCDataValidator(df)
    is_valid, errors = validator.validate()

    assert is_valid is False
    assert any("depósitos/transferencias con monto negativo" in e for e in errors)

def test_missing_mandatory_fields_fails(spark):
    data = [
        Row(transaction_type="DEPOSIT", amount=None, account_id="A1", event_time="2024-01-01T08:00:00"),
        Row(transaction_type="DEPOSIT", amount=100.0, account_id=None, event_time="2024-01-01T08:00:00"),
        Row(transaction_type="DEPOSIT", amount=100.0, account_id="A1", event_time=None)
    ]
    df = spark.createDataFrame(data)
    validator = CDCDataValidator(df)
    is_valid, errors = validator.validate()

    assert is_valid is False
    assert any("registros con campos obligatorios nulos" in e for e in errors)

def test_multiple_errors_detected(spark):
    data = [
        Row(transaction_type="WALLET_CREATED", amount=1.0, account_id="A1", event_time="2024-01-01T08:00:00"),
        Row(transaction_type="TRANSFER_IN", amount=-10.0, account_id="A1", event_time="2024-01-01T08:00:00"),
        Row(transaction_type="DEPOSIT", amount=None, account_id=None, event_time=None),
    ]
    df = spark.createDataFrame(data)
    validator = CDCDataValidator(df)
    is_valid, errors = validator.validate()

    assert is_valid is False
    assert len(errors) == 3
