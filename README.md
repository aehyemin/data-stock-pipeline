# Stock Data Pipeline Project


## 1. 프로젝트 개요
- **목표**: 미국 주식/ETF 가격 데이터를 수집, 적재, 정제하여 분석 가능하도록 제공


## 2.  프로젝트 스택

<img width="896" height="547" alt="Screenshot from 2025-08-26 20-20-26" src="https://github.com/user-attachments/assets/1b340e04-561c-409e-b7e4-21bc03434c06" />



1. **데이터 소스**
     - Kaggle의 데이터셋 활용. https://www.kaggle.com/datasets/borismarjanovic/price-volume-data-for-all-us-stocks-etfs
     - ETF/Stock의 `Date`, `Open`, `Close`, `Volume`, `OpenInt` 데이터 활용
       
2. **Ingestion & Orchestration(Airflow)**
    - DAG를 사용해 Kaggle -> GCS -> BigQuery 적재 자동화
    - `kaggle_to_gcs`: Kaggle 데이터셋을 GCS에 적재
    - `gcs_to_bigquery_bronze`: Bronze 테이블 생성(스키마 정의와 함께 `ticker`, `type` 컬럼 추가 후 GCS 데이터를 BigQuery에 적재)
    - `gcs_to_bigquery_silver`: 실버 테이블 생성(중복 제거, 이상치 제거 및 정제)

3. **Data Lake(GCS)**
   - Raw txt 파일 저장: `raw/source=kaggle/dataset=stock-price/type={stock,etf}/ticker={ticker_name}`
   - 재처리/백업 가능하도록 계층적 폴더 구조 설계

4. **Data Warehouse(BigQuery)**
   - `stocks_bronze(airflow)`: 원본 데이터 보관(`ticker`, `type` 컬럼 추가)
   - `stocks_silver(airflow)`: 정제된 데이터 보관
   - `staging(dbt)`: 컬럼명/스키마 표준화(view)
   - `core(dbt)`: 분석용 테이블 생성

5. **Transformation(dbt)**
   - `stg_stocks`: 실버 테이블을 snake_case로 변환
   - `models/core/monthly_returns`: 티커별 월별 수익률 계산
   - 데이터 품질 테스트
   - dbt docs generate로 문서화

6. **Visualization(Tableau)**
   - 특정 티커의 월간 수익률 비교 차트
   - Tableau Server에서 BigQuery 테이블을 데이터 소스로 연결
  



