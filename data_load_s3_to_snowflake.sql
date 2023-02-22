/* this code loads data from AWS S3 to Snowflake Database */

drop database esg_database1;
drop schema esg_schema1;
drop warehouse d2b_warehouse1;

use role accountadmin;
create database esg_database1;
use database esg_database1;
create  schema esg_schema1;
use schema esg_schema1;

create warehouse d2b_warehouse1 with
warehouse_size= Large
auto_resume=TRUE
initially_suspended=true;




alter warehouse d2b_warehouse1 resume;

  
create table esg_database1.esg_schema1.impact_table(
    Description string,
    Filter_Level_1 string,
    Filter_Level_2 string,
    Level int,
    CUSIP float,
    Portfolio float,
    Benchmark float,
    Active float,
    Portfolio_1 float,
    Benchmark_1 float,
    Active_1 float,
    Portfolio_2 float,
    Benchmark_2 float,
    Active_2 float,
    Sector_Allocation float,
    Security_Selection float,
    Reporting_Date date,
    Portfolio_Code string);
    
copy into esg_database1.esg_schema1.impact_table 
from s3://esg-preprocessed-data/impactR.csv credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key')
file_format = (type = csv field_delimiter = ',' skip_header = 1);



create table esg_database1.esg_schema1.reference_table(
    Fund_ID string,
    Fund_Name string,
    ESG_Policy_number int,
    Bmk_ID string,
    Bmk_Name string,
    Date date);
    
copy into esg_database1.esg_schema1.reference_table 
from s3://esg-preprocessed-data/referenceR.csv credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key')
file_format = (type = csv 
               field_delimiter = ',' 
               skip_header = 1
               FIELD_OPTIONALLY_ENCLOSED_BY = '"')
on_error = 'skip_file';


create table esg_database1.esg_schema1.Analytics_table(
    Portfolio VARCHAR,
    Filter_Level_1 VARCHAR,
    Level int,
    NNIP_Environment_Momentum int,
    NNIP_Environment_Score int,
    NNIP_Governance_Momentum int,
    NNIP_Governance_Score int,
    NNIP_Social_Momentum int,
    NNIP_Social_Score int,
    Highest_Controversy_Level_Answer_Category int,
    Sustainalytics_Total_Exposure_Score int,
    Sustainalytics_Total_ESG_Score int,
    Sustainalytics_Environment_Score int,
    Sustainalytics_Social_Score int,
    Sustainalytics_Governance_Score int,
    Sustainalytics_ESG_Momentum_score int,
    Sustainalytics_ESG_Risk_Momentum int,
    Sustainalytics_Environmental_Risk_Momentum int,
    Sustainalytics_Social_Risk_Momentum int,
    Sustainalytics_Governance_Risk_Momentum int,
    Sustainalytics_Unmanageable_Risk_Momentum int,
    Sustainalytics_Environmental_Risk_Score int,
    Sustainalytics_Social_Risk_Score int,
    Sustainalytics_Governance_Risk_Score int,
    Sustainalytics_Managed_Risk_Score int,
    Sustainalytics_Manageable_Risk_Score int,
    Sustainalytics_Unmanaged_Risk_Score int,
    Sustainalytics_Unmanageable_Risks_Score int,
    CO2_emissions_scope_1_2_intensity int,
    CO2_emissions_scope_1_2_3_intensity int,
    CO2_emissions_scope_3_intensity int,
    Waste_produced_intensity int,
    Water_consumed_intensity int,
    Sustainalytics_Management_Gap_Score int);
    
copy into esg_database1.esg_schema1.Analytics_table 
from s3://esg-preprocessed-data/AnalyticsR.csv credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key')
file_format = (type = csv 
               field_delimiter = ',' 
               skip_header = 1 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"')
on_error = 'SKIP_FILE';


create table esg_database1.esg_schema1.Active_attribution_table(
    Description string,
    Filter_Level_1 string,
    Filter_Level_2 string,
    Filter_Level_3 string,
    Level int,
    CUSIP varchar,
    Portfolio_weight float,
    Benchmark_weight float,
    Active_weight float,
    Portfolio_return float,
    Benchmark_return float,
    Active_return float,
    Portfolio_returncontribution float,
    Benchmark_returncontribution float,
    Active_returncontribution float,
    Sector_Allocation float,
    Security_Selection float,
    Reporting_Date date,
    Portfolio_Code string,
    time_lens string);
    
copy into esg_database1.esg_schema1.Active_attribution_table 
from s3://esg-preprocessed-data/active_attributionR.csv credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key')
file_format = (type = csv 
               field_delimiter = ',' 
               skip_header = 1)
on_error = 'SKIP_FILE';

SHOW GRANTS ON SCHEMA esg_schema;
SHOW GRANTS TO USER MELODYE; 
grant imported privileges on database esg_database to role accountadmin;
revoke imported privileges on database esg_database from role accountadmin;

drop table esg_database1.esg_schema1.stock_growth;
DROP schema esg_database1.ESG_SCHEMA1_ESG_;
