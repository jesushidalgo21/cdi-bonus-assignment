**README.md**

---

## Proyecto CDI Bonus Assignment

**Descripción breve**: Este proyecto implementa un pipeline en PySpark para calcular el bono CDI diario sobre los saldos de las billeteras de usuarios, almacenando saldos e intereses en PostgreSQL y ofreciendo un dashboard interactivo en Streamlit.

---

## Tabla de contenidos

1. [Visión General](#visión-general)
2. [Características Destacadas](#características-destacadas)
3. [Requisitos Previos](#requisitos-previos)
4. [Instalación y Configuración](#instalación-y-configuración)
5. [Ejecución del Pipeline](#ejecución-del-pipeline)
6. [Dashboard Interactivo](#dashboard-interactivo)
7. [Consultas con DuckDB y DBeaver](#consultas-con-duckdb-y-dbeaver)
8. [Conexión a PostgreSQL con DBeaver](#conexión-a-postgresql-con-dbeaver)
9. [Testing](#testing)
10. [Estructura de Directorios](#estructura-de-directorios)
11. [Contribuir](#contribuir)

---

## Visión General

Este pipeline cubre todo el flujo para:

* Ingesta de datos CDC de saldos de billeteras (raw Parquet en `data/cdc/`).
* Generación de historial de saldos diarios en PostgreSQL (tabla `wallet_history`).
* Obtención de tasas CDI desde la API del Banco Central de Brasil y almacenamiento en Parquet particionado en `data/cdi_rates/partition_date=YYYY-MM-DD/`.
* Cálculo diario (o por rango de fechas) de intereses sobre saldos inmovilizados ≥ 100 BRL, usando PySpark.
* Registro de pagos de interés (`interest_payments`) y auditoría granular en la tabla `interest_audit_log`.

---

## Características Destacadas

* **Modularidad**: Pipeline dividido en scripts independientes (`load_wallet_history`, `fetch_cdi_rates`, `calculate_interest_payments`).
* **Encapsulamiento**: Clases para manejo de Spark (`SparkSessionManager`), logging (`Logger`), validación de datos (`CDCDataValidator`), fetcher de CDI y auditoría (`InterestAuditLogger`).
* **Flexibilidad en ejecución**: Soporta ejecución diaria (solo `--start`) o por ventana (`--start` y `--end`).
* **Almacenamiento**:

  * **CDC**: Parquet en `data/cdc/`.
  * **Tasas CDI**: Parquet particionado en `data/cdi_rates/partition_date=YYYY-MM-DD/`.
  * **PostgreSQL**: Tablas particionadas por fecha para `wallet_history`, `interest_payments` y `interest_audit_log`.
* **Auditoría**: Registro detallado de cada decisión de cálculo de interés en `interest_audit_log`.
* **Dashboard**: Página en Streamlit para monitorear resúmenes, detalles y tendencias de pagos e incidencias.
* **Consultas offline**: Recomendación de uso de DuckDB con DBeaver para explorar los Parquet locales de CDC o CDI.
* **Integración con API del BCB**: Obtención de tasas CDI diarias y cálculo de interés basado en fracción de año (365 días).

---

## Requisitos Previos

* **Docker y Docker Compose** (versión 20.10.7 o superior)
  - [Guía de instalación para Windows](https://docs.docker.com/desktop/install/windows-install/)
  - [Guía de instalación para Mac](https://docs.docker.com/desktop/install/mac-install/)
  - [Guía de instalación para Linux](https://docs.docker.com/engine/install/)
  
* Java 11 (requerido por Spark)
* Python 3.9+ (solo necesario para desarrollo local fuera de Docker)
* DBeaver (o cliente SQL similar) para exploración de datos
---

## Instalación y Configuración

1. **Instalar Docker** (si no lo tienes):
   - Sigue las guías oficiales según tu sistema operativo en los links de Requisitos Previos
   - Después de instalar, inicia Docker Desktop y espera a que esté completamente operativo

2. Clonar el repositorio:
   ```bash
   git clone https://github.com/jesushidalgo21/cdi-bonus-assignment.git
   cd cdi-bonus-assignment
3. Revisar y ajustar `config/config.yaml` si fuera necesario (rutas, credenciales PostgreSQL).
4. Construir y levantar servicios con Docker Compose:

   ```bash
   docker-compose up --build
   ```

---

## Ejecución del Pipeline

Para ejecutar el pipeline dentro del contenedor Docker:

```bash
docker exec -it pyspark-container python3 run_pipeline.py --start YYYY-MM-DD [--end YYYY-MM-DD]
```

* Sin `--end`: calcula solo la fecha de inicio.
* Con `--end`: calcula el rango de fechas inclusivo.

Ejemplos:
```bash
docker exec -it pyspark-container python3 run_pipeline.py --start 2024-08-28
```

```bash
docker exec -it pyspark-container python3 run_pipeline.py --start 2024-05-01 --end 2024-10-07
```
* Recomendación: Se recomienda ejecutar el segundo ejemplo, ya que la fuente de datos CDC contiene información dentro de ese rango específico (2024-05-01 a 2024-10-07). Si el pipeline se ejecuta con fechas fuera de este intervalo, no habrá datos de origen disponibles para su procesamiento, lo que podría generar errores en la ejecución.

El pipeline corre en tres pasos:

1. **load\_wallet\_history**: Genera historial diario en PostgreSQL.
2. **fetch\_cdi\_rates**: Descarga tasas CDI del BCB y guarda en Parquet.
3. **calculate\_interest\_payments**: Calcula y guarda pagos de interés y auditoría.

---

## Dashboard Interactivo

* Accede en: `http://localhost:8501`
* **Filtros**: rango de fechas, tipo de análisis (Resumen General, Detalle por Cuenta, Tendencias Temporales).
* **Resumen General**: métricas, proporción de cuentas calificadas, tasa de error.
* **Detalle por Cuenta**: lista de pagos de interés con filtros.
* **Tendencias Temporales**: gráficos de evolución de montos y correlaciones.

---

## Consultas con DuckDB y DBeaver

Para explorar Parquet de CDC o CDI localmente, se recomienda usar DBeaver ([https://dbeaver.io/](https://dbeaver.io/)) con el plugin DuckDB:

1. Instala DuckDB CLI como driver en DBeaver.
2. En DBeaver, crea una nueva conexión seleccionando "DuckDB" y apunta al directorio de Parquet:

   * Para CDC: `data/cdc/*.parquet`
   * Para CDI Rates: `data/cdi_rates/partition_date=*/part-*.parquet`
3. Ejecuta tus consultas SQL de manera interactiva, sin necesidad de scripts Python.

---

## Conexión a PostgreSQL con DBeaver

También puedes usar DBeaver para conectarte a la base de datos PostgreSQL:

1. Crea una nueva conexión PostgreSQL en DBeaver con los datos de `config/config.yaml`:

   * Host: `localhost` (o `postgres` si usas Docker Compose)
   * Puerto: `5432`
   * Base de datos: `wallet_db`
   * Usuario: `wallet_user`
   * Contraseña: `wallet_pass`
2. Explora las tablas particionadas en el esquema `wallet`:

   * `wallet_history`
   * `interest_payments`
   * `interest_audit_log`

---

## Testing

Para ejecutar tests desde el contenedor:

```bash
docker-compose run tests
```

Incluye pruebas unitarias de validación, cálculo de intereses y generación de historial.

---

## Estructura de Directorios

```text
.
├── config/             # Configuración YAML
├── data/               # Datos de entrada y tasas
│   ├── cdc/            # Parquet CDC raw
│   └── cdi_rates/      # Parquet tasas CDI particionado
├── dashboard/          # App Streamlit
├── docs/               # Documentación adicional
├── postgres/           # Scripts de inicialización
├── src/                # Código fuente modular
│   ├── jobs/           # Scripts de pipeline
│   ├── audit_logger.py
│   ├── fetcher.py
│   ├── interest_calculation.py
│   └── ...
├── tests/              # Pruebas unitarias
├── Dockerfile
├── docker-compose.yml
├── run_pipeline.py     # Orquestador
└── README.md
```

---

## Contribuir

1. Haz un fork del repositorio.
2. Crea una rama (`git checkout -b feature/nueva-funcionalidad`).
3. Realiza tus cambios y tests.
4. Envía un Pull Request describiendo tu aporte.

---

*¡Gracias por revisar este proyecto! Cualquier sugerencia será bienvenida.*
