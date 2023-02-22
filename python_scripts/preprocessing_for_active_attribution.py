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


s3Resource = boto3.resource('s3')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='d2b-sdbx-c7928bc42d13.json'
FILENAME = ''

class Extract:
    def __init__(self,driver,host,port,user,password,database):
        self.driver = driver
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
    def connect(self):
        try:
            self.conn = psycopg2.connect("db= '{}' user= '{}' password='{}' port='{}'".format(
                self.database, self.user, self.host, self.port, self.password)
                                         )
            self.cur = self.conn.cursor()
        except Exception as e:
            print (e)
            
            
    def ingest_data(self):
        bucket_name='d2b-internal-spark-project-source-data'
        filename = '30_10_2020.zip'
        source_file_name = f'Input/Dataproc/{filename}'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_file_name)
        blob.download_to_filename(filename)
        temp_dir = pathlib.Path(filename.split('.')[0])
        with zipfile.ZipFile(filename) as zip_file:
            zip_file.extractall(temp_dir)
            contents = zip_file.namelist()
            for content in contents:
                if content.endswith('.zip'):
                    content_extraction = io.BytesIO(zip_file.read(content))
                    zips = zipfile.ZipFile(content_extraction)
                    for files in zips.namelist():
                        zips.extract(files, temp_dir / content.split('.')[0])
                    zips.close()
                    f = temp_dir/content
                    f.unlink()

columns_dict = {
    "Filter Level 1": "Filter_Level_1",
    "Filter Level 2": "Filter_Level_2",
    "Filter Level 3": "Filter_Level_3",
    "Portfolio": "Portfolio_weight",
    "Benchmark": "Benchmark_weight",
    "Active": "Active_weight",
    "Portfolio (bp)": "Portfolio_return",
    "Benchmark (bp)": "Benchmark_return",
    "Active (bp)": "Active_return",
    "Portfolio (bp).1": "Portfolio_returncontribution",
    "Benchmark (bp).1": "Benchmark_returncontribution",
    "Active (bp).1": "Active_returncontribution",
    "Sector Allocation (bp)": "Sector_Allocation",
    "Security Selection (bp)": "Security_Selection",
    "Portfolio_from_file": "Portfolio"
}

reverse_column_dict = {value: key for key, value in columns_dict.items()}

def f(x):
    try:
        if x < -1 or x > 1000000:
            return 0
        return x
    except:
        return x



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
                date = header.loc[0, "Date"]
                portfolio_short_name = header.loc[0, "Portfolio Short Name"]

                columns = df.iloc[3, :].values
                df = df.iloc[4:, :]
                df.columns = columns
                df.rename(columns=columns_dict, inplace=True)

                d = re.compile('\(([12])\)')
                reg = re.findall(d, sheet)

                TIME_LENS_DICT = {
                    '0': 'MTD',
                    '1': 'QTD',
                    '2': 'YTD'
                }

                lens_value = reg[0] if reg else '0'

                df = df.assign(
                    Reporting_Date=date, 
                    Portfolio_Code=portfolio_short_name,
                    time_lens=TIME_LENS_DICT[lens_value]
                )
                dfs.append(df)
            df = pd.concat(dfs)
            df = df[(df["Level"] != 1) & (df["Level"] != 2)]
            df.reset_index(inplace=True)
            df.drop('index', axis=1, inplace=True)
            df.to_excel('processed.xlsx', index=False)

            df.to_csv("Active Attribution processed.csv", index=False)


            return df


# len([1, 2, 3, 4])
preprocessing  = Preprocess("./30_10_2020/Analytics")

data = preprocessing.preprocess()
print(data)
