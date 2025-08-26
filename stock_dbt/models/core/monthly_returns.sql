-- to table in Bigquery
{{ config(
        materialized = "view"
    )
}}

-- 월간 수익률 계산에 필요한 컬럼 
with base as (
  select
    DATE(date) as trade_date,  
    ticker,
    close
  from {{ ref('stg_stocks') }}
),

-- 티커,월 별 마지막 거래일
month_last as (
  select
    ticker,
    DATE_TRUNC(trade_date, MONTH) as month_label,
    MAX(trade_date) as eom_date
  from base
  group by 1, 2
),

-- 마지막 거래일의 종가
eom_price as (
  select
    m.ticker,
    m.month_label,
    m.eom_date,
    b.close as eom_close
  from month_last m
  join base b
    on b.ticker = m.ticker
   and b.trade_date = m.eom_date
)

--전월말 대비 월간 수익률
select
  ticker,
  month_label,
  eom_date,
  eom_close,
  LAG(eom_close) OVER (partition by ticker order by month_label) as prev_eom_close,
  SAFE_DIVIDE(eom_close,
              LAG(eom_close) OVER (partition by ticker order by month_label)) - 1
    as monthly_return
from eom_price
order by ticker, month_label;