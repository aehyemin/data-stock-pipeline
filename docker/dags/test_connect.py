from __future__ import annotations
from datetime import datetime
import os, logging

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def gcs_ping():
    from google.cloud import storage


    bucket_name = Variable.get("GCS_BUCKET", default_var=os.getenv("GCS_BUCKET"))
    logging.info("GCS_BUCKET Variable=%r Env=%r",
                 Variable.get("GCS_BUCKET", default_var=None),
                 os.getenv("GCS_BUCKET"))
    if not bucket_name:
        raise ValueError("GCS_BUCKET not set (Variable or env)")

    client = storage.Client() 
    exists = client.bucket(bucket_name).exists()
    if not exists:
        raise RuntimeError(f"Bucket not found or no access: {bucket_name}")

    blobs = list(client.list_blobs(bucket_name, max_results=5))
    print("OK GCS:", bucket_name, "first_blobs:", [b.name for b in blobs])
    return True


with DAG(
    dag_id="connection_test_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:
    kaggle_connection_test = BashOperator(
        task_id="kaggle_connection_test",
        bash_command="kaggle competitions list | head -n 5"
    )
    gcp_connection_test = PythonOperator(
        task_id="gcp_connection_test",
        python_callable=gcs_ping,
    )
    kaggle_connection_test >> gcp_connection_test
