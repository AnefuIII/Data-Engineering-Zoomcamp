#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

import subprocess


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db #database
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    subprocess.run(['wget', url, '-O', f'{csv_name}.gz'], check=True)
    subprocess.run(['gzip', '-d', f'{csv_name}.gz'], check=True)
    
    #os.system(f'wget {url} -o {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, chunksize = 100000, iterator = True)
    # with open(csv_name, 'rb') as f: 
    #     df_iter = pd.read_csv(f, chunksize=100000, iterator=True, compression='gzip')

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #inout first batch first
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df = next(df_iter) 
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print('inserted another chunk... took %.3f' % (t_end - t_start))

        except StopIteration:
            print("Reached the end of the data.")
            break



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Injest csv data to postgres')

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url
    parser.add_argument('--user', help = 'username for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'post for postgres')
    parser.add_argument('--db', help = 'database for postgres')
    parser.add_argument('--table_name', help = 'name of table to write results to')
    parser.add_argument('--url', help = 'url of the csv')

    args = parser.parse_args()

    main(args)





