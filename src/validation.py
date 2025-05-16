from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs
from src.logger import Logger

logger = Logger().get_logger()

class CDCDataValidator:
    """Validador de datos CDC con reglas de negocio específicas"""
    REQUIRED_FIELDS = ["account_id", "event_time", "amount"]
    TRANSACTION_TYPES = {
        "WALLET_CREATED": "WALLET_CREATED",
        "DEPOSIT": "DEPOSIT",
        "TRANSFER_IN": "TRANSFER_IN"
    }

    def __init__(self, df: DataFrame):
        """
        Inicializa el validador con un DataFrame de datos CDC
        
        Args:
            df (DataFrame): DataFrame con datos CDC a validar
        """
        self.df = df
        self.errors = []
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Ejecuta todas las validaciones configuradas
        
        Returns:
            tuple[bool, list[str]]: (True si es válido, lista de mensajes de error)
        """
        logger.info("Iniciando validación completa de datos CDC")
        
        self._validate_wallet_creation()
        self._validate_deposit_transactions()
        self._validate_required_fields()
        
        is_valid = len(self.errors) == 0
        validation_result = (is_valid, self.errors.copy())
        
        if is_valid:
            logger.info("✅ Todos los datos CDC pasaron la validación")
        else:
            logger.error(f"❌ Validación fallida con {len(self.errors)} error(es)")
        
        return validation_result
    
    def _validate_wallet_creation(self) -> None:
        """Valida que cuentas creadas no tengan montos distintos de cero"""
        invalid_records = self.df.filter(
            (col("transaction_type") == self.TRANSACTION_TYPES["WALLET_CREATED"]) &
            (spark_abs(col("amount")) > 0.001)
        )
        
        count = invalid_records.count()
        if count > 0:
            message = f"⚠️ {count} cuentas creadas con monto distinto de cero"
            self.errors.append(message)
            logger.warning(message)
    
    def _validate_deposit_transactions(self) -> None:
        """Valida que depósitos no tengan montos negativos"""
        invalid_records = self.df.filter(
            col("transaction_type").isin(
                self.TRANSACTION_TYPES["DEPOSIT"],
                self.TRANSACTION_TYPES["TRANSFER_IN"]
            ) &
            (col("amount") < 0)
        )
        
        count = invalid_records.count()
        if count > 0:
            message = f"⚠️ {count} depósitos/transferencias con monto negativo"
            self.errors.append(message)
            logger.warning(message)
    
    def _validate_required_fields(self) -> None:
        """Valida que campos obligatorios no sean nulos"""
        # Verificar existencia de columnas
        missing_cols = [f for f in self.REQUIRED_FIELDS if f not in self.df.columns]
        if missing_cols:
            message = f"❌ Faltan columnas requeridas: {missing_cols}"
            self.errors.append(message)
            logger.error(message)
            return
        
        # Verificar valores nulos
        null_condition = None
        for field in self.REQUIRED_FIELDS:
            cond = col(field).isNull()
            null_condition = cond if null_condition is None else null_condition | cond
        
        invalid_records = self.df.filter(null_condition)
        count = invalid_records.count()
        
        if count > 0:
            message = f"⚠️ {count} registros con campos obligatorios nulos"
            self.errors.append(message)
            logger.warning(message)
