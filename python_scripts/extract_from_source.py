import pandas as pd
from google.cloud import storage
import os
import io
from datetime import datetime, timedelta
import bz2, os, sys
from pandas_gbq import to_gbq, read_gbq
import boto3
import zipfile
s3Resource = boto3.resource('s3')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='d2b-sdbx-c7928bc42d13.json'
FILENAME = ''

# Extract data from Cloud storage 
class Extract:
    def __init__(self,driver,host,port,user,password,database):
        self.driver = driver
        self.host = host
        self.port = port
        self.user = password
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
        now = datetime.now() # current date and time
        date_time = now- timedelta(days=1)
        # filename = 'FILE-NAME'+datetime.strftime(date_time , "%d%m%Y")+'.csv'
        #print("date and time:",filename)

        bucket_name='d2b-internal-spark-project-source-data'
        source_file_name = 'Input/Dataproc/30_10_2020.zip'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_file_name)
        data = blob.download_as_bytes()
        #print(data)
        archive= io.BytesIO()
        archive.write(data)
        with zipfile.ZipFile(archive,'wb') as zip_file:
            zip_file.extractall()
            
            
            
storage_ = Extract('','','','','','')
storage_.ingest_data()

