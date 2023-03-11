import os
# import io
# import zipfile
# import requests
# import itertools
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from prefect import flow, task



def construct_url(service: str, year: int, month: int) -> str:
    """
    Constructs a URL for a given service and year and month.
    """
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'

def construct_file_path(service: str, year: int, month: int) -> str:
    """
    Constructs a file path for a given service and year and month.
    """
    os.makedirs(f'./data/{service}/{year}', exist_ok=True)
    return f'./data/{service}/{year}/{service}_tripdata_{year}-{month:02d}.parquet'

def construct_table_name(service: str, year: int, month: int) -> str:
    """
    Constructs a table name for a given service and year and month.
    """
    return f'{service}_tripdata_{year:04d}_{month:02d}'

def setup_database():
    """
    Sets up the database.
    """
    # load environment variables
    load_dotenv()
    
    # get environment variables
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')
    
    # connect to database
    engine = sa.create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    
    # create stage schema if it doesn't exist
    con = engine.connect()
    con.execute('CREATE SCHEMA IF NOT EXISTS staging')
    
    # cleanup
    con.close()
    engine.dispose()

@task(log_prints=True, retries=3, retry_delay_seconds=1)
def fetch_dataset(url: str, filepath: str) -> pd.DataFrame:
    """
    Downloads a dataset from a URL and returns it as a Pandas DataFrame.
    """
    # if file already exists, skip
    if os.path.exists(filepath+'.gz') or os.path.exists(filepath):
        print('\n', f'File {filepath} already exists.'.center(90), '\n')
        return
    # else download
    print('\n', f'Downloading {url} to {filepath}...'.center(90), '\n')
    os.system(f'wget {url} -O {filepath}')
    print('\n', '\nDownload complete.'.center(90), '\n')
    return

@task(log_prints=True, retries=3, retry_delay_seconds=1)
def rename_columns(filepath: str, service: str) -> pd.DataFrame:
    """
    Renames columns in a data file.
    """
    try:
        # read in data
        df = pd.read_parquet(filepath)
    except FileNotFoundError:
        # read in compressed data
        df = pd.read_parquet(filepath+'.gz')
        
    # formatter function
    def column_formatter(name: str):
        return name.replace('PUL','pickup_l').replace('DOL','dropoff_l').lower().replace('tpep_','').replace('lpep_','')
    
    # construct old and new column names
    old = df.columns.to_list().copy()
    new = [*map(column_formatter, old.copy())]

    # rename columns
    df.rename(columns=dict(zip(old,new)), inplace=True)

    return df

@task(log_prints=True, retries=3, retry_delay_seconds=1)
def replace_data_file(df: pd.DataFrame, filepath) -> pd.DataFrame:
    """
    Replaces a data file with a compressed parquet file.
    """
    # if file already exists, skip
    if os.path.exists(filepath+'.gz'):
        return pd.read_parquet(filepath+'.gz')
    # remove regular file
    os.remove(filepath)
    # compress data
    df.to_parquet(filepath+'.gz', compression='gzip')
    return pd.read_parquet(filepath+'.gz')

@task(log_prints=True, retries=3, retry_delay_seconds=5)
def upload_to_postgres(df: pd.DataFrame, tablename: str, schema: str = 'staging'):
    """
    Load data into PostgreSQL database.
    """
    # load environment variables
    load_dotenv()
    # 
    try:
        # get environment variables
        username = os.getenv('PG_USERNAME')
        password = os.getenv('PG_PASSWORD')
        host = os.getenv('PG_HOST')
        port = os.getenv('PG_PORT')
        dbname = os.getenv('PG_DATABASE')
        # connect to database
        engine = sa.create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        # if file already exists, skip
        if sa.inspect(engine).has_table(tablename, schema=schema):
            print('\n', f'Table {tablename} already exists.'.center(90), '\n')
            return
        
        # load data into database
        df.to_sql(tablename, engine, schema=schema, if_exists='fail', index=False, chunksize=100_000)

        # cleanup
        engine.dispose()
        return 
    
    except Exception as e:
        print(e)
        print(f'Failed to load data into {tablename}.')

@flow
def etl_subflow(args: tuple):
    """
    ETL subflow for the NYC Taxi Trip Data dataset.
    """
    # unpack arguments
    year, month, service = args
    print('\n', f'Processing {service} taxi trip data for: ({month:02d}/{year})'.center(90), '\n')
    
    # construct URL
    url = construct_url(service, year, month)
    # construct file path
    filepath = construct_file_path(service, year, month)
    # construct table name
    tablename = construct_table_name(service, year, month)


    # fetch dataset
    fetch_dataset(url, filepath)

    # rename columns
    df = rename_columns(filepath, service)
    # replace data file with compressed parquet file
    df = replace_data_file(df, filepath)

    # upload to postgres
    upload_to_postgres(df, tablename)

@flow
def etl_taxi_trips(year: int):
    """
    ETL flow for the NYC Taxi Trip Data dataset.
    """
    setup_database()
    # run flow
    for month in range(1, 13):
        # run flow
        etl_subflow((year, month, 'yellow'))
        # run flow
        etl_subflow((year, month, 'green'))



if __name__ == '__main__':
    print()
    etl_taxi_trips(2020)
    print()
