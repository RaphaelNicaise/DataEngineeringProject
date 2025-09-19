#!/bin/bash
set -e

# 1. Migrar la DB y crear conexiones y usuario
echo "Migrando base de datos Airflow..."
airflow db init
airflow db migrate
airflow connections create-default-connections

# Crear usuario admin (si no existe)
airflow users create \
    --username "${AIRFLOW_USER}" \
    --password "${AIRFLOW_PASSWORD}" \
    --firstname "Admin" \
    --lastname "User" \
    --role "Admin" \
    --email "admin@example.com" || true

# 2. Levantar webserver y scheduler
echo "Iniciando webserver y scheduler..."
airflow webserver &
sleep 5
echo "El webserver de Airflow está iniciado en http://localhost:8085"
airflow scheduler
