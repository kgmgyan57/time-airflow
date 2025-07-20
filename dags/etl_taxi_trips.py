from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

# Define variables
project_id_value = 'dottime-bronze'
dataset_name = 'time_landing'
table_name = 'taxi_trips_raw'
gcs_bucket_name = 'dottime-landing-raw'
gcs_uri = f'gs://{gcs_bucket_name}/*'
table_ref = f"{project_id_value}.{dataset_name}.{table_name}"

# Default arguments for the DAG
default_args = {
    "owner": "gyan",
    "description": "Raw Trips Pipeline Dag",
    "depend_on_past": False,
    "start_date": datetime(2025, 7, 19),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'gcs_to_bigquery_parquet_load',
    default_args=default_args,
    description='Load parquet files from GCS to BigQuery with monthly partitioning',
    schedule_interval=None,
    catchup=False,
    tags=['raw_taxi_trips', 'etl'],
)

# Task 1: Check if files exist in GCS bucket
check_gcs_files = GCSObjectsWithPrefixExistenceSensor(
    task_id='check_gcs_files_exist',
    bucket=gcs_bucket_name,
    prefix='',
    google_cloud_conn_id='google_cloud_default',
    timeout=300,
    poke_interval=60,
    dag=dag,
)

# Task 2: Load data from GCS parquet files to BigQuery with partitioning
load_parquet_to_bq = BigQueryInsertJobOperator(
    task_id='load_parquet_to_bigquery',
    configuration={
        "load": {
            "sourceUris": [gcs_uri],
            "destinationTable": {
                "projectId": project_id_value,
                "datasetId": dataset_name,
                "tableId": table_name,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_APPEND",
            "autodetect": True,
            "timePartitioning": {
                "type": "MONTH",
                "field": "time_start_timestamp",
            },
        }
    },
    google_cloud_conn_id='google_cloud_default',
    dag=dag,
)

# Define task dependencies
check_gcs_files >> load_parquet_to_bq