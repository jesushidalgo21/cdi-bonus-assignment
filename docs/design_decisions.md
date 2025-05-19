**docs/design\_decisions.md**

---

## Decisiones de Diseño y Recomendaciones de Producción

Este documento resume las principales decisiones técnicas tomadas en el desarrollo del pipeline CDI Bonus Assignment y ofrece sugerencias para su evolución hacia un entorno de producción.

---

### 1. Arquitectura General

* **Flujo de datos**:

  1. **CDC Inicia** en archivos Parquet (`data/cdc/`).
  2. **Wallet History**: Generación de balances diarios con `WalletHistoryGenerator`.
  3. **Tasas CDI**: Descarga desde API del Banco Central de Brasil (`fetcher.CDIDataFetcher`) y almacenamiento en Parquet particionado (`data/cdi_rates/partition_date=YYYY-MM-DD/`).
  4. **Cálculo de Intereses**: Unión de saldos y tasas, lógica de elegibilidad (`InterestCalculator`), escritura en PostgreSQL y auditoría.
  5. **Dashboard**: Monitorización de resultados y métricas.

* **Justificación**: Esta secuencia garantiza trazabilidad completa y separación de responsabilidades.

---

### 2. Elección de Tecnologías

| Capa              | Tecnología                 | Motivación                                           |
| ----------------- | -------------------------- | ---------------------------------------------------- |
| Procesamiento     | **Apache Spark (PySpark)** | Escalabilidad, tolerancia a fallos y paralelismo.    |
| Almacenamiento    | **PostgreSQL**             | Almacenamiento transaccional, particiones por fecha. |
| Exploración local | **DuckDB + DBeaver**       | Consultas rápidas sobre Parquet sin código.          |
| Dashboard         | **Streamlit + Plotly**     | Desarrollo rápido de UI interactiva.                 |
| API externa       | **Requests**               | Ligero y fácil de integrar con Spark.                |

---

### 3. Modularización y Encapsulamiento

* **SparkSessionManager**: Single‑ton para inicializar y limpiar SparkSession, evita múltiples sesiones.
* **Logger**: Logger centralizado con encabezados visuales para distinguir fases del pipeline.
* **Jobs desacoplados**: Cada script (`load_wallet_history`, `fetch_cdi_rates`, `calculate_interest_payments`) corre independientemente, facilitando orquestación con Airflow o Databricks Workflows.
* **Clases de negocio**: `CDCDataValidator`, `WalletHistoryGenerator`, `CDIDataFetcher`, `InterestCalculator`, `InterestAuditLogger` encapsulan lógica específica.

**Beneficio**: Facilita pruebas unitarias y mantenibilidad.

---

### 4. Manejo de Fechas y Particiones

* **Particiones por `partition_date`**:

  * En Parquet: facilita exploración por DuckDB.
  * En PostgreSQL: partición RANGE asegura performance y limpieza de datos antiguos.
* **Parámetros CLI**: `--start` y `--end` para control granular de ventanas de ejecución.
* **Recomendación**: En producción, usar particiones dinámicas y modo `append` o `merge` para no sobrescribir particiones existentes. Activar `spark.sql.sources.partitionOverwriteMode = 'dynamic'`.

---

### 5. Estrategia de Escritura en PostgreSQL

* Actualmente se usa `mode="overwrite"` para simplicidad. En producción:

  * **Append incremental** para particiones nuevas.
  * **Upserts** en la tabla de auditoría si es necesario evitar duplicados.
  * Configurar índices por partición y VACUUM/FILLFACTOR adecuados.

---

### 6. Logging y Auditoría

* **InterestAuditLogger**: Registra cada registro procesado, con campos de `qualified`, `reason`, `relevant_balance`, `calculated_interest`, `process_status` y `error_description`.
* **Tabla `interest_audit_log`**: Permite detectar rápidamente inconsistencias o errores de cálculo.
* **Recomendación**:

  * Integrar con un sistema de logs central (ELK, Datadog) para capturar niveles de log (`info`, `warning`, `error`, `critical`).
  * Generar alertas automáticas cuando se registren errores o un umbral de warnings sea superado.
  * Registrar métricas adicionales, como tiempos de ejecución por job y conteo de registros procesados.

---

### 7. Dashboard y Monitoreo

* **Streamlit**: Rápido despliegue de UI para:

  * Resumen de registros y tasa de éxito.
  * Gráficos de distribución y tendencias.
* **Optimización**: Connection pooling (`psycopg2.pool`) y cache TTL para reducir latencia.
* **Recomendación**:

  * Desplegar el dashboard en un entorno gestionado (AWS ECS/Fargate, Heroku) con HTTPS y autenticación.
  * Para necesidades más avanzadas de reporting y BI, considerar herramientas como **Power BI**, **Tableau** o **Otros**, que permiten crear dashboards empresariales con gobernanza de datos, escalabilidad y permisos de usuario más granulares.

---

### 8. Integración con API del BCB

* **Formato de fechas**: Conversión `YYYY-MM-DD` → `DD/MM/YYYY`.
* **Cálculo de tasa diaria**: Dividir `cdi_rate` anual por 365.
* **Almacenamiento**: Parquet particionado para cada fecha consultada.
* **Recomendación**: Manejar claves API y rate limits con un módulo de retry (exponential backoff). Cachear respuestas si la frecuencia de ejecución es alta.

---

### 9. Consultas Offline con DuckDB

* **Uso de DBeaver**: cómodo para explorar Parquet sin escribir código Python.
* **Ventaja**: análisis rápido, joins ad-hoc y prototipado.

---

### 10. Orquestación y Automatización

* **run\_pipeline.py** con `subprocess` simula un workflow sencillo.
* **Producción**: Reemplazar por Airflow, Prefect o Databricks Workflows para:

  * Programación diaria.
  * Retries automáticos.
  * Dependencias explícitas.

---

### 11. Módulo de Notificaciones (Feature futura)

* Se consideró implementar alertas por email ante errores o finalización exitosa.
* **Trade‑off**: Requiere credenciales SMTP o servicio de terceros (SendGrid, SES).
* **Recomendación**: Incorporar un módulo de notificaciones desacoplado (emailer) con patrones pub/sub o callbacks en Airflow; enviar resumen de ejecución y tasa de error.

---

### 12. Trade‑offs y Compromisos

| Área                     | Decisión actual      | Consideración de producción         |
| ------------------------ | -------------------- | ----------------------------------- |
| Escritura en Postgres    | `overwrite` completo | Incremental/upsert por partición    |
| Orquestación             | Subprocess sequence  | Airflow / Prefect / Databricks      |
| Notificaciones           | No implementado      | Módulo email o Slack integrable     |
| Gestión de configuración | YAML local           | Secret manager (AWS Secrets, Vault) |

---

### 13. Próximos Pasos

1. **Migración a orquestador** (Airflow).
2. **Escritura incremental** por partición en PostgreSQL.
3. **Alertas y notificaciones** integradas.
4. **Escalado del dashboard** con autenticación.
5. **Mejoras en validaciones** (esquemas, calidad de datos).

### Extra: Ingestión en Base Transaccional

Para integrar los datos calculados en un sistema transaccional en tiempo real o casi real:

- **Microservicio RESTful**: Se puede exponer un endpoint (por ejemplo con FastAPI) que reciba los pagos de interés y los inserte directamente en la base de datos de producción. Esto permitiría actualizaciones inmediatas sin esperar al batch diario.
- **Mensajería / Kafka**: Publicar cada registro de `interest_payments` en un tópico de Kafka usando un conector Spark o un producer simple; luego usar Kafka Connect para volcar en PostgreSQL en modo sink connector.
- **CDC y Debezium**: Si el pipeline inicial se alimenta de un sistema OLTP, se puede usar Debezium para capturar cambios y mantener sincronizadas las tablas de `wallet_history` e `interest_payments`.
- **Consideraciones**: Asegurar transacciones atómicas, idempotencia de los servicios y manejo de errores/retries en caso de fallos de red o deadlocks.

---

*Fin del documento.*
