import os
import psycopg2
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
    return f'{service}_tripdata_{year}'

@task
def setup_database(schema: str = 'trips_data_all'):
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
    with psycopg2.connect(user=username, password=password, host=host, port=port, dbname=dbname) as conn:
        # create stage schema if it doesn't exist
        c = conn.cursor()
        c.execute('CREATE SCHEMA IF NOT EXISTS trips_data_all')

        # cleanup
        conn.commit()

@task(log_prints=True)
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

@task(log_prints=True)
def replace_data_file(filepath: str) -> pd.DataFrame:
    """
    Replaces a data file with a compressed parquet file.
    """
    # if file already exists, skip
    if os.path.exists(filepath+'.gz'):
        return pd.read_parquet(filepath+'.gz')
    # else read from file
    df = pd.read_parquet(filepath)
    # remove regular file
    os.remove(filepath)
    # compress data
    df.to_parquet(filepath+'.gz', compression='gzip')
    return pd.read_parquet(filepath+'.gz')

@task(log_prints=True)
def upload_to_postgres(df: pd.DataFrame, tablename: str, schema: str = 'trips_data_all') -> None:
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
        df.to_sql(tablename, engine, schema=schema, if_exists='append', index=False, chunksize=100_000)

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

    # replace data file with compressed parquet file
    df = replace_data_file(filepath)

    # upload to postgres
    upload_to_postgres(df, tablename)

@flow
def etl_taxi_trips(year: int):
    """
    ETL flow for the NYC Taxi Trip Data dataset.
    """
    setup_database()
    # iterate through the months of a year
    for month in range(1, 13):
        # run flow for yellow taxis
        etl_subflow((year, month, 'yellow'))
        # run flow for green taxis
        etl_subflow((year, month, 'green'))



if __name__ == '__main__':
    print()
    etl_taxi_trips(2019)
    # etl_taxi_trips(2020)
    # etl_taxi_trips(2021)
    print()
