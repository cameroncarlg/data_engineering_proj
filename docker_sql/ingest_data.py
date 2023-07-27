#!/usr/bin/env python


import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # We would download the csv right here

    # This is to create a connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Iterator to iterate through the csv, adding a chunk at a time.
    # You can only hold so much data in RAM at once, needs to be chunked out
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # df = next iteration of iterator
    df = next(df_iter)

    # Convert the timestamp datatype
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Extracting the schema for the db
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')



    while True:
        try:
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    parser.add_argument('--user', help='username for pg')
    parser.add_argument('--password', help='password for pg')
    parser.add_argument('--host', help='host for pg')
    parser.add_argument('--port', help='port for pg')
    parser.add_argument('--db', help='database name for pg')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)

    

