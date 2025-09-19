FROM apache/airflow:2.7.1

USER root

# Instalamos Java y herramientas de compilación
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiamos requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Instalamos librerías Python
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt