import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
excel_file = pd.read_excel("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/preprocess_ready_for_s3/ana.xlsx")
print(excel_file)

excel_file.rename(columns = {
    'Filter Level 1':'Filter_Level_1', 
    'NNIP Environment Momentum(Wt Avg-PORT NMV/MV)':'NNIP_Environment_Momentum',
    'NNIP Environment Score(Wt Avg-PORT NMV/MV)':'NNIP_Environment_Score',
    'NNIP Governance Momentum(Wt Avg-PORT NMV/MV)':'NNIP_Governance_Momentum',
    'NNIP Governance Score(Wt Avg-PORT NMV/MV)':'NNIP_Governance_Score',
    'NNIP Social Momentum(Wt Avg-PORT NMV/MV)':'NNIP_Social_Momentum',
    'NNIP Social Score(Wt Avg-PORT NMV/MV)':'NNIP_Social_Score',
    'Highest Controversy Level-Answer Category(Wt Avg-PORT NMV/MV)': 'Highest_Controversy_Level_Answer_Category',
    'Sustainalytics Total Exposure Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Total_Exposure_Score',
    'Sustainalytics Total ESG Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Total_ESG_Score',
    'Sustainalytics Environment Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Environment_Score',
    'Sustainalytics Social Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Social_Score',
    'Sustainalytics Governance Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Governance_Score',
    'Sustainalytics ESG Momentum score(Wt Avg-PORT NMV/MV)':'Sustainalytics_ESG_Momentum_score',
    'Sustainalytics ESG Risk Momentum(Wt Avg-PORT NMV/MV)':'Sustainalytics_ESG_Risk_Momentum',
    'Sustainalytics Environmental Risk Momentum(Wt Avg-PORT NMV/MV)':'Sustainalytics_Environmental_Risk_Momentum',
    'Sustainalytics Social Risk Momentum(Wt Avg-PORT NMV/MV)':'Sustainalytics_Social_Risk_Momentum',
    'Sustainalytics Governance Risk Momentum(Wt Avg-PORT NMV/MV)':'Sustainalytics_Governance_Risk_Momentum',
    'Sustainalytics Unmanageable Risk Momentum(Wt Avg-PORT NMV/MV)':'Sustainalytics_Unmanageable_Risk_Momentum',
    'Sustainalytics Environmental Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Environmental_Risk_Score',
    'Sustainalytics Social Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Social_Risk_Score',
    'Sustainalytics Governance Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Governance_Risk_Score',
    'Sustainalytics Managed Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Managed_Risk_Score',
    'Sustainalytics Manageable Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Manageable_Risk_Score',
    'Sustainalytics Unmanaged Risk Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Unmanaged_Risk_Score',
    'Sustainalytics Unmanageable Risks Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Unmanageable_Risks_Score',
    'CO2 emissions scope 1&2 intensity(Wt Avg-PORT NMV/MV)':'CO2_emissions_scope_1_2_intensity',
    "CO2 emissions scope 1, 2 & 3 intensity(Wt Avg-PORT NMV/MV)":'CO2_emissions_scope_1_2_3_intensity',
    'CO2 emissions scope 3 intensity(Wt Avg-PORT NMV/MV)':'CO2_emissions_scope_3_intensity',
    'Waste produced intensity(Wt Avg-PORT NMV/MV)':'Waste_produced_intensity',
    'Water consumed intensity(Wt Avg-PORT NMV/MV)':'Water_consumed_intensity',
    'Sustainalytics Management Gap Score(Wt Avg-PORT NMV/MV)':'Sustainalytics_Management_Gap_Score'}, inplace = True)

#df = excel_file.to_csv("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/ana.csv")

#print(df)

#df1= df.drop(df.columns[-1], axis=1, inplace=True)
excel_file = excel_file[excel_file.columns[:-2]]
print(excel_file)
#df1= pd.read_csv("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/new_file.csv")
df = excel_file.to_csv("ana.csv")

#remove the first column-preprocesssing
with open ('AnalyticsR.csv', 'w') as out_file :
    with open ('ana.csv') as in_file :
        for line in in_file :
            test_string = line.strip ('\n').split (',')
            out_file.write (','.join (test_string [1:]) + '\n')
print("AnalyticsR.csv")



