#!/usr/bin/env python
# coding: utf-8
#author:Rohan patankar


import pandas as pd
import sqlalchemy
from time import time
import argparse
import os

class AppendDataToPostgres:
    def main(self,params):
        user=params.user
        password=params.password
        host=params.host
        port=params.port
        database=params.database
        table=params.table
        file_path=params.file_path
        # output_file="output.csv"
        
        create_engine=sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        create_engine
        data=pd.read_csv(file_path)
        pd.io.sql.get_schema(data,name="New_York_Data",con=create_engine)
        data1=pd.read_csv(file_path,iterator=True,chunksize=100000)
        while True:
            try:
                start_time=time()
                df=next(data1)
                df["pickup_datetime"]=pd.to_datetime(df["pickup_datetime"])
                df.to_sql(name=table,con=create_engine,if_exists="append")
                end_time=time()
                total_time=round((end_time-start_time),2)
                print(f"Chunks appending took {total_time} seconds")
            except StopIteration as e:
                print("All data appended successfully")
                break
            
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest data to postgres.')
    parser.add_argument("--user", help="Postgres user name")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres host")
    parser.add_argument("--port", help="Postgres port")  
    parser.add_argument("--database", help="Postgres database")
    parser.add_argument("--table", help="Postgres table")
    parser.add_argument("--file_path", help="File to ingest")
    args = parser.parse_args()
    AppendDataToPostgres().main(args)