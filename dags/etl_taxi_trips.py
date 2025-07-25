from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Variables
project_id_value = 'dottime-bronze'
dataset_name = 'time_landing'
table_name = 'taxi_trips_raw'
gcs_bucket_name = 'dottime-landing-raw'
gcs_uri_prefix = ''  # Adjust if files are in a subfolder

default_args = {
    "owner": "gyan",
    "description": "Raw Trips Pipeline Dag",
    "depend_on_past": False,
    "start_date": datetime(2025, 7, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='gcs_to_bigquery_parquet_load',
    default_args=default_args,
    schedule_interval="0 22 * * *",
    catchup=False,
    tags=['raw_taxi_trips', 'etl'],
) as dag:

    load_parquet_to_bigquery = GCSToBigQueryOperator(
        task_id='load_parquet_to_bigquery',
        bucket=gcs_bucket_name,
        source_objects=['*'],
        destination_project_dataset_table=f'{project_id_value}.{dataset_name}.{table_name}',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        autodetect=True,
        time_partitioning={
            "type": "MONTH",
            "field": "trip_start_timestamp"
        },
        gcp_conn_id='google_cloud_default',
    )

    load_parquet_to_bigquery