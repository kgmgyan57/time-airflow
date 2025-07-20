from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client.models import V1Volume, V1VolumeMount

dag_id = "dbt_celebi__core_trips"


default_args = {
    "owner": "gyan",
    "description": "Fact Trips Pipeline Dag",
    "depend_on_past": False,
    "start_date": datetime(2025, 7, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

pod_args = {
    "project_id": "dottime-bronze",
    "cluster_name": "autopilot-cluster-dtm",
    "location": "us-central1",
    "namespace": "default",
    "image": "us-docker.pkg.dev/dottime-bronze/dottime-dbt-celebi/dbt-celebi:prod",
    "image_pull_policy": "Always",
    "get_logs": True,
    "on_finish_action": "delete_succeeded_pod",
    "log_events_on_failure": True,
    "trigger_rule": TriggerRule.ALL_SUCCESS,
    "volumes": [V1Volume(name="config-files", config_map={"name": "dbt-configmap"})],
    "volume_mounts": [
        V1VolumeMount(name="config-files", mount_path="/root/.dbt", read_only=True)
    ],
    "startup_timeout_seconds": 600,
    "name": "DBT_models",
    "cmds": ["/bin/bash", "-c"],
}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={"fullrefresh": ""},
) as dag:

    start_dag = EmptyOperator(task_id="start_dag")

    end_dag = EmptyOperator(task_id="end_dag")

    run_core_trips = GKEStartPodOperator(
        **pod_args,
        task_id="fact_trips_pipeline",
        execution_timeout=timedelta(minutes=60),
        arguments=[
            "dbt build {{params.fullrefresh or ''}} --select +tag:core --exclude +fact_taxi_trips +dim_dates --target bronze_prod;"
        ],
    )

    (
        start_dag
        >> run_core_trips
        >> end_dag
    )