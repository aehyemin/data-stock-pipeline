{{ config(
        materialized = "view"
    )
}}

SELECT 
    Date AS Date,
    Open AS open,
    High AS high,
    Low AS low,
    Close AS close,
    Volume AS volume,
    type,
    UPPER(ticker) AS ticker
FROM {{ source('stocks_silver', 'silver_table')}}