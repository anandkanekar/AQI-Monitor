-- One Postgres instance backs both the Hive Metastore and Airflow (separate databases).
CREATE DATABASE metastore;
CREATE DATABASE airflow;
