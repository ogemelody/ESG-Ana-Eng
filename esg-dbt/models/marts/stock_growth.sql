{{ config(materialized='table',schema = 'ESG_FINANCE')}}
    select 
        STOCK_NAME,
        CATEGORY,
        BENCHMARK_WEIGHT,
        ACTIVE_WEIGHT,
        PORTFOLIO_RETURN,
        BENCHMARK_RETURN,
        ACTIVE_RETURN,
        PORTFOLIO_RETURNCONTRIBUTION,
        BENCHMARK_RETURNCONTRIBUTION,
        ACTIVE_RETURNCONTRIBUTION
    
    from {{ref('stg_active_attribtion')}}
