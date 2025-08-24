
from datetime import datetime
import json
from airflow import DAG
from pandas import json_normalize
from airflow.models import Variable
from pathlib import Path
import zipfile
import os 
from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


#사용할 변수 가져오기
GCS_BUCKET = Variable.get("GCS_BUCKET")
KAGGLE_DATASET = Variable.get("KAGGLE_DATASET")

#다운로드될 파일: price-volume-data-for-all-us-stocks-etfs
FILE_NAME = KAGGLE_DATASET.split('/')[1]


#업로드할 경로 지정
UPLOAD_ROUTE = "raw/source=kaggle/dataset=stock-price"


#임시 작업 경로. airflow 공식 이미지의 작업루트가 /opt/airflow라 그 아래 디렉토리를 만들어 사용
TMP_ROUTE = "/opt/airflow/tmp/kaggle_data"
ZIP_ROUTE = f"/opt/airflow/tmp/{FILE_NAME}.zip" #내려받을 집파일 경로


#stock인지 etf인지 타입 추출
def etf_or_stock(path: str) -> str:
    name = {i for i in Path(path).parts}
    if "ETFs" in name:
        return "etf"
    if "Stocks" in name:
        return "stock"
    return "unknown"
    

#티커 이름 추출
def ticker_name(filename: str) -> str | None:
    # "qqq.us.txt" -> QQQ
    nam = Path(filename).name
    suffix = ".us.txt"

    if not nam.endswith(suffix):
        return None
    
    ticker = nam[: -len(suffix)]

    return ticker.upper()


#gcs에 저장할 파일 경로 설정
def to_gcs(original: str, upload_route:str) -> str:
    ticker = ticker_name(original) or "Unknown"
    typ = etf_or_stock(original)
    file = Path(original).name

    return f"{upload_route}/type={typ}/ticker={ticker}/{file}"


#zip파일 압축 해제. 압축을 풀고 -> .txt 뽑아내기
def extract_zip(zip_route:str, dir:str) -> str:
    Path(dir).mkdir(parents=True, exist_ok=True)

    cnt = 0

    with zipfile.ZipFile(zip_route) as z: #파일 내의 모든 파일 이름을 가져와 name변수에 할당
        for name in z.namelist():
            low = name.lower()
            if low.endswith(".txt") and ("/stocks/" in low or "/etfs/" in low):
                z.extract(name, dir) #name에 해당하는 파일을, dir에 압축해제
                cnt += 1

    if cnt == 0:
        raise RuntimeError("zip안에 .txt 파일이 없음")
    
    return str(Path(dir) / "Data")

#.txt 파일을 버킷에 순서대로 업로드. 중복없이

def upload_to_gcs(local_dir: str, bucket_name:str, upload_route:str, skip_existing:bool=True) -> dict:
    client = storage.Client()
    bck = client.bucket(bucket_name)

    upload = 0
    skip = 0
    
    for p in Path(local_dir).rglob("*.txt"):
        p = str(p)
        key = to_gcs(p, upload_route) #경로 생성
        blob = bck.blob(key)

        if skip_existing and blob.exists():
            skip += 1
            continue

        blob.upload_from_filename(p)
        upload += 1

    print("summaary", {"uploaded": upload, "skip": skip})
    return {"uploaded": upload, "skip": skip}



with DAG(
    dag_id = "kaggle_to_gcs",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags = ["ingest", "kaggle", "gcs"],
) as dag:


    download_zip = BashOperator(
        task_id = "download_zip",
        bash_command=(
            "mkdir -p /opt/airflow/tmp && "
            "kaggle datasets download -d {{var.value.KAGGLE_DATASET}} "
            "--force -p /opt/airflow/tmp"
        ),
    )


    def extract_and_upload():
        data_root = extract_zip(ZIP_ROUTE, TMP_ROUTE)

        summary = upload_to_gcs(
            local_dir=data_root,
            bucket_name=GCS_BUCKET,
            upload_route=UPLOAD_ROUTE,
            skip_existing=True,
        )
        print("완료", summary)
        return summary

    extract_and_upload_task = PythonOperator(
        task_id="extract_and_upload",
        python_callable = extract_and_upload
    )

    download_zip >> extract_and_upload_task