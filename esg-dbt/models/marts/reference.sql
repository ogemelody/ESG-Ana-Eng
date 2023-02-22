{{ config(materialized='table',schema = 'ESG_FINANCE')}}
    select *
    
    from {{ref('stg_reference')}}
