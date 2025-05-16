FROM openjdk:11-jre-slim

# ---- Etapa de configuración base ----
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    DATA_PATH=/data \
    APP_HOME=/app

# Instala dependencias mínimas
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# ---- Instalación de Spark ----
RUN mkdir -p /opt \
    && wget -q "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
    && rm /tmp/spark.tgz

# Variables de entorno Spark
ENV SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$PATH \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

    # ---- Instala driver JDBC de PostgreSQL ----
ENV POSTGRES_JDBC_VERSION=42.7.3
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" -O /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# ---- Configuración de usuario y directorios ----
RUN groupadd -r spark && useradd -r -g spark -m -d /home/spark spark \
    && chown -R spark:spark /opt/spark \
    && mkdir -p $APP_HOME $CDC_PATH $OUTPUT_PATH /home/spark/.ivy2/cache \
    && chown -R spark:spark $APP_HOME $CDC_PATH $OUTPUT_PATH /home/spark

# ---- Instalación de dependencias ----
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt duckdb==0.10.0
RUN pip install psycopg2-binary  # Para conexiones Python

# ---- Copia de código y permisos ----
WORKDIR $APP_HOME
COPY --chown=spark:spark . $APP_HOME

# Puerto para Spark UI (debugging)
EXPOSE 4040

USER spark

# Comando por defecto (modo interactivo)
CMD ["bash"]