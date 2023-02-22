
select 
    FUND_ID,
    FUND_NAME,
    ESG_POLICY_NUMBER,
    BMK_ID as Banking_market,
    BMK_NAME as Banking_market_name,
    DATE as DATE
from {{ source('raw_data', 'REFERENCE_TABLE') }}