from datetime import datetime
from airflow import DAG
from airflow.models import Variable

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

#변수
BQ_PROJECT   = Variable.get("BQ_PROJECT")
BQ_DATASET   = Variable.get("BQ_DATASET")        
BRONZE_TABLE = "bronze_table"  

SILVER_TABLE = "silver_table"
SILVER_DATASET= "stocks_silver"

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["silver", "bigquery", "transform"],
) as dag:  

    #1. 실버 데이터셋 생성
    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = "create_silver_dataset",
        project_id= BQ_PROJECT,
        dataset_id= SILVER_DATASET,
        location="asia-northeast3",
        exists_ok=True,
    )

    #2. 브론즈 -> 실버 변환하기
    #음수 제거, 비정상 가격 high <= low
    transform_bronze_to_siver = BigQueryInsertJobOperator(
        task_id="transform_bronze_to_silver",
        gcp_conn_id="google_cloud_default",
        location="asia-northeast3",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{BQ_PROJECT}.{SILVER_DATASET}.{SILVER_TABLE}`
                PARTITION BY DATE_TRUNC(Date, MONTH)
                CLUSTER BY type, ticker
                AS
                WITH src AS (
                    SELECT
                        SAFE_CAST(Date as DATE) AS Date,
                        SAFE_CAST(Open as FLOAT64) AS Open,
                        SAFE_CAST(High as FLOAT64) AS High,
                        SAFE_CAST(Low as FLOAT64) AS Low,
                        SAFE_CAST(Close as FLOAT64) AS Close,
                        SAFE_CAST(Volume as INT64) AS Volume,
                        COALESCE(SAFE_CAST(OpenInt AS INT64), 0) AS OpenInt,
                        NULLIF(TRIM(type), "") AS type,
                        UPPER(NULLIF(TRIM(ticker), "")) AS ticker

                    FROM `{BQ_PROJECT}.{BQ_DATASET}.{BRONZE_TABLE}` 
                    ),

                    
                    qc AS (
                        SELECT *
                        FROM src
                        WHERE
                            Date IS NOT NULL
                            AND ticker IS NOT NULL
                            AND High IS NOT NULL
                            AND Low IS NOT NULL
                            AND High >= Low
                            AND (Volume IS NULL OR Volume >= 0)
                    ),
                    remo AS (
                        SELECT *
                        FROM qc
                            QUALIFY ROW_NUMBER() OVER (
                                PARTITION BY Date, ticker
                                ORDER BY Date DESC
                            ) = 1

                )
                SELECT
                    Date, Open, High, Low, Close, Volume, OpenInt, type, ticker
                FROM remo
                """,
                "useLegacySql": False,
            }
        },
    )



    create_silver_dataset >> transform_bronze_to_siver