FROM apache/airflow:3.3.1-python3.12

# ============================================================
# Versions
# ============================================================
ARG SPARK_VERSION=4.2.0

# ============================================================
# System dependencies + Java
# ============================================================
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ============================================================
# Apache Spark
# ============================================================
RUN curl -fL \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark \
    && rm -f /tmp/spark.tgz \
    && chown -R airflow:root /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ============================================================
# Python packages
# ============================================================
USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    "apache-airflow-providers-apache-spark==6.3.1" \
    "dbt-postgres==1.11.0" \
    "pyarrow==25.0.0" \
    "psycopg2-binary"

RUN pip check
