from datetime import datetime
from airflow import DAG
from airflow.models import Variable

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)


#사용할 변수 가져오기
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_PROJECT = Variable.get("BQ_PROJECT")
BQ_DATASET = Variable.get("BQ_DATASET")
BRONZE_TABLE   = "bronze_table"  #테이블 이름

UPLOAD_PREFIX = "raw/source=kaggle/dataset=stock-price" #gcs 폴더 경로

SCHEMA_FIELDS = [
            {"name": "Date", "type": "DATE" },
            {"name": "Open", "type": "FLOAT" },
            {"name": "High", "type": "FLOAT"},
            {"name": "Low", "type": "FLOAT"},
            {"name": "Close", "type": "FLOAT"},
            {"name": "Volume", "type": "INT64"},
            {"name": "OpenInt", "type": "INT64"},
            {"name": "type",   "type": "STRING"}, 
            {"name": "ticker", "type": "STRING"}, 

]

with DAG(
    dag_id="gcs_to_bigquery_bronze",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["load", "bigquery", "bronze"],

) as dag:
    #1. 브론즈 데이터셋 생성. 없으면 생성
    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        project_id= BQ_PROJECT,
        dataset_id= BQ_DATASET,
        exists_ok=True,
    )


    #2. GCS -> bigquery로 적재
    load_to_table = BigQueryInsertJobOperator(
        task_id= "load_from_gcs",
        gcp_conn_id="google_cloud_default",
      
        configuration= {
            "load": {
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BRONZE_TABLE,
            },
            "sourceUris": [f"gs://{GCS_BUCKET}/{UPLOAD_PREFIX}/*"],
            "sourceFormat": "CSV",
            "skipLeadingRows": 1, #첫번째 행은 컬럼이므로 생략
            "fieldDelimiter": ",",
            "autodetect": False,
            "schema": {"fields": SCHEMA_FIELDS},
            "timePartitioning": {"type": "MONTH", "field": "Date"},
            "clustering": {"fields": ["type", "ticker"]},

            "hivePartitioningOptions": { #데이터를 로드하기 이전에 사용
                "mode": "AUTO", 
                "sourceUriPrefix": f"gs://{GCS_BUCKET}/{UPLOAD_PREFIX}",
                #위 경로 다음을 key=value로 인식
                #key(type) = value(etf), key(ticker)=value(QQQ)
            },
        "writeDisposition": "WRITE_TRUNCATE",
        }
        },
    )
    create_bronze_dataset >> load_to_table



