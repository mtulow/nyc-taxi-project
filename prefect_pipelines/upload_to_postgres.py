import os
import glob
import pandas as pd
import sqlalchemy as sa
import multiprocessing as mp
from dotenv import load_dotenv



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
    if month == 1:
        df.to_sql(tablename, con=engine, schema='trips_data_all', if_exists='replace', index=False, chunksize=100_000)
    else:
        df.to_sql(tablename, con=engine, schema='trips_data_all', if_exists='append', index=False, chunksize=100_000)
    print('\n', f'Uploaded {os.path.basename(filepath)} to {tablename}', '\n')


def load_data(year: int)-> pd.DataFrame:
    """
    Loads the taxi data data for the given year.

    Args:
        year: The year of the data.

    Return:
        The data for the given year.
    """
    # get the data files
    yellow_files = get_data_files(service='yellow', year=year)
    print(f'Found {len(yellow_files)} yellow taxi data files for {year}')
    green_files = get_data_files(service='green', year=year)
    print(f'Found {len(green_files)} green taxi data files for {year}')
    data_files = yellow_files + green_files
    
    print()
    
    # 
    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(upload_to_postgres, data_files)
    pool.close()




if __name__ == '__main__':
    print()
    load_data(2019)
    print()
    