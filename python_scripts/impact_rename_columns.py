import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
parquet_file = pd.read_excel("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/imp.xlsx")
print(parquet_file)
columns_dict={
    "Description": "Description",
    "Filter Level 1": "Filter_Level_1",
    "Filter Level 2": "Filter_Level_2",
    "Level": "Level",
    "CUSIP": "CUSIP",
    "Portfolio": "Portfolio",
    "Benchmark float": "Benchmark_float",
    "Active": "Active",
    "Portfolio.1": "Portfolio_1",
    "Benchmark.1" : "Benchmark_1",
    "Active.1": "Active_1",
    "Portfolio.2": "Portfolio_2",
    "Benchmark.2": "Benchmark_2",
    "Active.2":  "Active_2",
    "Sector Allocation" :  "Sector_Allocation",
    "Security Selection" : "Security_Selection",
    "Reporting_Date": "Reporting_Date",
    "Portfolio_Code": "Portfolio_Code"}

reverse_column_dict = {value: key for key, value in columns_dict.items()}

output_path = parquet_file.to_csv("C:/Users/melod/OneDrive/Documents/Data2bots/new_ESG_project/result.csv")
print(output_path)

#remove the first column-preprocesssing
with open ('new_file.csv', 'w') as out_file :
    with open ('result.csv') as in_file :
        for line in in_file :
            test_string = line.strip ('\n').split (',')
            out_file.write (','.join (test_string [1:]) + '\n')
print("new_file.csv")
