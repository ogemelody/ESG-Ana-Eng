import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
excel_file = pd.read_excel("Reference_30-09-2020.xlsx")
print(excel_file)
excel_file.rename(columns = {'Fund ID':'Fund_ID', 'Fund Name':'Fund_Name','ESG Policy number':'ESG_Policy_number','Bmk ID':'Bmk_ID','Bmk Name':'Bmk_Name'}, inplace = True)


output_path = excel_file.to_csv("reference.csv")
print(output_path)

#remove the first column-preprocesssing
with open ('referenceR.csv', 'w') as out_file :
    with open ('reference.csv') as in_file :
        for line in in_file :
            test_string = line.strip ('\n').split (',')
            out_file.write (','.join (test_string [1:]) + '\n')
print("referenceR.csv")