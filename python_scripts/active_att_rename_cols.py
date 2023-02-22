import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
#excel_file = pd.read_excel("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/preprocess_ready_for_s3/active_attribution.csv")
#print(excel_file)
excel_file= pd.read_csv("act_attR.csv")
#df = excel_file.to_csv("act_attR.csv")
print(excel_file)

excel_file.rename(columns = {
    'Description':'Description', 
    'Filter_Level_1':'Filter_Level_1',
    'Filter_Level_2':'Filter_Level_2',
    'Filter_Level_3':'Filter_Level_3',
    'Level':'Level',
    'CUSIP':'CUSIP',
    'Portfolio_returncontribution':'Portfolio_weight',
    'Benchmark_returncontribution': 'Benchmark_weight',
    'Active_returncontribution':'Active_weight',
    'Portfolio_weight.1':'Portfolio_return',
    'Benchmark_weight.1':'Benchmark_return',
    'Active_weight.1':'Active_return',
    'Portfolio_weight.2':'Portfolio_returncontribution',
    'Benchmark_weight.2':'Benchmark_returncontribution',
    'Active_weight.2':'Active_returncontribution',
    'Sector Allocation':'Sector_Allocation',
    'Security Selection':'Security_Selection'}, inplace = True)

df = excel_file.to_csv("act_attR2.csv")

with open ('active_attributionR.csv', 'w') as out_file :
    with open ('act_attR2.csv') as in_file :
        for line in in_file :
            test_string = line.strip ('\n').split (',')
            out_file.write (','.join (test_string [1:]) + '\n')
#print("act_attR.csv")