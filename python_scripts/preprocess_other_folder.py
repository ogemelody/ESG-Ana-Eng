import pandas as pd
from google.cloud import storage
import os
import io
from datetime import datetime, timedelta
import bz2, os, sys
from pandas_gbq import to_gbq, read_gbq
import boto3
import zipfile
import pathlib
import psycopg2
import re


class Preprocess:
    #     "remove headers and footers"
    def __init__(self, filepath):
        self._filepath = filepath

    
    def preprocess(self) -> pd.DataFrame:
        dfs = []
        path = pathlib.Path(self._filepath)
        files = path.glob("**/*.xlsx")
        # print(list(files))
        # Loop through folder
        for file in files:
            excel = pd.ExcelFile(file) # read excel file
            sheets = excel.sheet_names

            print(sheets)

            for sheet in sheets:
                df = excel.parse(sheet, skiprows=2, skipfooter=3)
                header = df.iloc[:1, :10]
                
            df.to_csv("Analytics processed.csv", index=False)


            return df
preprocessing  = Preprocess("./30_10_2020/Analytics")
