import os
import glob
import time
import psycopg2
import pandas as pd
import datetime as dt
import sqlalchemy as sa
import multiprocessing as mp
from dotenv import load_dotenv



def setup_database(year: int):
    """
    Sets up the postgres database.
    """
    # load environment variables
    load_dotenv()

    # get environment variables
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')

    # connect to postgres
    with psycopg2.connect(dbname=dbname, user=username, password=password, host=host, port=port) as conn:
        with conn.cursor() as c:
            # create schema if it doesn't exist
            c.execute("CREATE SCHEMA IF NOT EXISTS trips_data_all;")

            # drop tables if they exist
            c.execute(f"DROP TABLE IF EXISTS trips_data_all.yellow_tripdata_{year} CASCADE;")
            c.execute(f"DROP TABLE IF EXISTS trips_data_all.green_tripdata_{year} CASCADE;")


def get_data_files(service: str = None, year: int = None)-> list[str]:
    """
    Returns a list of data files for a given service and year and month.

    Args:
        service: The service name.
        year: The year of the data.
        month: The month of the data.

    Return:
        A list of data files.
    """
    print(f"Getting {service} taxi data files for {year}")
    # defaults
    service = service or '*'
    year = year or '*'
    return glob.glob(f'data/{service}/{year}/*.gz')

def upload_to_postgres(filepath: str)-> None:
    """
    Uploads a file to the postgres database.

    Args:
        filepath: The path to the file to upload.
    """
    # load environment variables
    load_dotenv()

    # get environment variables
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')
    # connect to postgres
    engine = sa.create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    
    # extract the taxi service, year, and month from the filepath
    filename = os.path.basename(filepath).removesuffix('.parquet.gz').split('_')
    service = filename[0]
    year, month = map(int, filename[-1].split('-'))
    # construct the table name
    tablename = f'{service}_tripdata_{year}'
    
    # read data file
    df = pd.read_parquet(filepath, engine='fastparquet')
    # load data to postgres
    print(f"Uploading {os.path.basename(filepath)} into PostgreSQL table {tablename}")
    t0 = time.perf_counter()
    df.to_sql(tablename, con=engine, schema='trips_data_all', if_exists='append', index=False, chunksize=10_000)
    t1 = time.perf_counter()
    print(f'Uploaded {os.path.basename(filepath)} to {tablename}, total time: {dt.timedelta(seconds=t1-t0)}')
    

def load_data(year: int)-> pd.DataFrame:
    """
    Loads the taxi data data for the given year.

    Args:
        year: The year of the data.

    Return:
        The data for the given year.
    """
    # initialize the database
    setup_database(year)

    # get the data files
    yellow_files = get_data_files(service='yellow', year=year)
    print(f'Found {len(yellow_files)} yellow taxi data files for {year}')
    green_files = get_data_files(service='green', year=year)
    print(f'Found {len(green_files)} green taxi data files for {year}')
    data_files = yellow_files + green_files
    
    print()
    
    # load the data
    t_start = time.perf_counter()
    pool = mp.Pool(processes=2 or mp.cpu_count())
    pool.map(upload_to_postgres, data_files)
    t_end = time.perf_counter()
    
    print()
    
    print(f'Uploaded {len(data_files)} taxi data files for {year}, total time: {dt.timedelta(seconds=t_end-t_start)}')
    






if __name__ == '__main__':
    print()
    load_data(2019)
    print()
    