#!/bin/bash
set -e

echo "Starting Postgres initialization..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Create airflow user
    CREATE USER airflow WITH PASSWORD 'airflow';

    -- Create databases
    CREATE DATABASE airflow OWNER airflow;
    CREATE DATABASE celery_results_db OWNER airflow;
    CREATE DATABASE elt_db OWNER airflow;

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE celery_results_db TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE elt_db TO airflow;

    -- Set timezone for airflow database to UTC
    ALTER DATABASE airflow SET timezone TO 'UTC';
EOSQL

echo "Postgres databases and users created successfully"
