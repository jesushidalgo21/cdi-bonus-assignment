from pyspark.sql import functions as F

def validate_cdc_data(df):
    """
    Valida reglas críticas en los datos CDC antes de procesar.
    Retorna (es_valido: bool, mensajes_error: list[str])
    """
    errors = []
    
    # Regla 1: No hay cuentas creadas con monto != 0
    invalid_creations = df.filter(
        (F.col("transaction_type") == "WALLET_CREATED") & 
        (F.abs(F.col("amount")) > 0.001)  # Evita falsos positivos por floats
    )
    if invalid_creations.count() > 0:
        errors.append(f"⚠️ {invalid_creations.count()} cuentas creadas con monto distinto de cero")
    
    # Regla 2: No hay montos negativos en DEPOSIT/TRANSFER_IN
    invalid_deposits = df.filter(
        F.col("transaction_type").isin("DEPOSIT", "TRANSFER_IN") & 
        (F.col("amount") < 0)
    )
    if invalid_deposits.count() > 0:
        errors.append(f"⚠️ {invalid_deposits.count()} depósitos con montos negativos")
    
    # Regla 3: Integridad de campos obligatorios
    missing_fields = df.filter(
        F.col("account_id").isNull() | 
        F.col("event_time").isNull() | 
        F.col("amount").isNull()
    )
    if missing_fields.count() > 0:
        errors.append(f"⚠️ {missing_fields.count()} registros con campos obligatorios nulos")
    
    return (len(errors) == 0, errors)