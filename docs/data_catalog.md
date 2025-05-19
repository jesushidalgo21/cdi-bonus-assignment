**docs/data\_catalog.md**

---

## Catálogo de Datos

Este documento describe las entidades (tablas) principales que conforman el pipeline CDI Bonus Assignment, con sus columnas, tipos y descripción de negocio.

---

### 1. Esquema: `wallet`

Todas las tablas se encuentran en el esquema `wallet` de PostgreSQL.

---

### 2. Tabla: `wallet.wallet_history`

| Columna              | Tipo SQL      | Descripción                                                                                          |
| -------------------- | ------------- | ---------------------------------------------------------------------------------------------------- |
| `account_id`         | UUID          | Identificador único de la cuenta de usuario.                                                         |
| `user_id`            | UUID          | Identificador único del usuario propietario de la cuenta.                                            |
| `balance`            | DECIMAL(18,2) | Saldo al final del día (EOD) de la cuenta.                                                           |
| `balance_previous`   | DECIMAL(18,2) | Saldo al final del día anterior.                                                                     |
| `immovable_balance`  | DECIMAL(18,2) | Parte del saldo que no se movió en las últimas 24 horas y es elegible para cálculo de interés.       |
| `last_movement_ts`   | TIMESTAMP     | Marca de tiempo de la última transacción o movimiento en la cuenta.                                  |
| `total_deposits`     | DECIMAL(18,2) | Suma de todos los depósitos realizados ese día.                                                      |
| `total_withdrawals`  | DECIMAL(18,2) | Suma de todos los retiros realizados ese día (valor negativo transformado a positivo en el cálculo). |
| `transactions_count` | INTEGER       | Número total de transacciones registradas ese día.                                                   |
| `partition_date`     | DATE          | Fecha de partición, corresponde al día de los datos.                                                 |

**Particionada por**: `PARTITION BY RANGE (partition_date)`

---

### 3. Tabla: `wallet.interest_payments`

| Columna            | Tipo SQL      | Descripción                                                                      |
| ------------------ | ------------- | -------------------------------------------------------------------------------- |
| `payment_id`       | BIGSERIAL     | Identificador secuencial de pago de interés.                                     |
| `user_id`          | UUID          | Identificador de usuario beneficiario del pago de interés.                       |
| `account_id`       | UUID          | Identificador de la cuenta asociada.                                             |
| `eligible_balance` | DECIMAL(10,2) | Saldo inmovilizado elegible para generar interés.                                |
| `interest_rate`    | DECIMAL(5,4)  | Tasa CDI diaria aplicada (fracción anual /365).                                  |
| `interest_amount`  | DECIMAL(10,2) | Monto de interés calculado para esa fecha y cuenta.                              |
| `is_paid`          | BOOLEAN       | Indicador de si el pago ya se marcó como efectivamente abonado.                  |
| `payment_date`     | TIMESTAMP     | Fecha y hora del registro del pago en el sistema.                                |
| `partition_date`   | TEXT          | Cadena con la fecha de partición (YYYY-MM-DD) para particionamiento de la tabla. |

**Particionada por**: `PARTITION BY RANGE (partition_date)`

---

### 4. Tabla: `wallet.interest_audit_log`

| Columna               | Tipo SQL      | Descripción                                                          |
| --------------------- | ------------- | -------------------------------------------------------------------- |
| `log_id`              | BIGSERIAL     | Identificador secuencial del registro de auditoría.                  |
| `job_id`              | VARCHAR       | UUID de la ejecución (batch) para agrupar registros de un mismo job. |
| `job_timestamp`       | TIMESTAMP     | Marca de tiempo de inicio de ejecución del job.                      |
| `user_id`             | UUID          | Identificador de usuario evaluado.                                   |
| `account_id`          | UUID          | Identificador de cuenta evaluada.                                    |
| `qualified`           | BOOLEAN       | Indicador si la cuenta calificó para generación de interés.          |
| `reason`              | TEXT          | Razón de calificación o no calificación.                             |
| `relevant_balance`    | DECIMAL(10,2) | Saldo usado para calcular interés (immovable\_balance).              |
| `calculated_interest` | DECIMAL(10,2) | Monto de interés calculado antes de guardarlo.                       |
| `interest_rate`       | DECIMAL(5,4)  | Tasa CDI diaria aplicada.                                            |
| `process_status`      | TEXT          | Estado del proceso (`success` ó `error`).                            |
| `error_description`   | TEXT          | Detalle del error en caso de `process_status = 'error'`.             |
| `partition_date`      | DATE          | Fecha de partición para auditoría, coincide con la fecha de cálculo. |

**Particionada por**: `PARTITION BY RANGE (partition_date)`

---

### 5. Fuentes de Datos Externas

* **Archivos CDC**: Parquet en `data/cdc/` (raw change data capture de billeteras).
* **Parquet de Tasas CDI**: Archivos generados en `data/cdi_rates/partition_date=YYYY-MM-DD/`.

---

### 6. Notas Adicionales

* Para explorar tablas de PostgreSQL y Parquet localmente, se recomienda usar **DBeaver** con drivers DuckDB y PostgreSQL.
* El catálogo puede extenderse incluyendo vistas materializadas o tablas de métricas derivadas.

---
