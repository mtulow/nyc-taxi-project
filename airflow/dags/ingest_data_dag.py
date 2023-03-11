import os
import wget
import pandas as pd
import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



    


def upload_to_postgres(filename: str, tablename: str, schema: str = 'public'):
    """
    Load data into PostgreSQL database.
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
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    # read parquet file
    df = pd.read_parquet(filename)
    # remove original file
    os.remove(filename)
    # load data into database
    df.to_sql(tablename, engine, if_exists='append', index=False, chunksize=100_000)
    # store data as compressed parquet file
    df.to_parquet(filename+'.gz', compression='gzip')




local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=dt.datetime(2021, 1, 1)
)


with local_workflow:

    
    for service in ['yellow', 'green']:
        # construct url
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}' + '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
        # construct filename
        filename = os.path.join('data', service, url.split('/')[-1])
        # construct tablename
        tablename = f'{service}_tripdata'

        # create data directory
        setup_local_ingestion = BashOperator(
            task_id="setup_local_ingestion",
            bash_command=f"mkdir -p data/{service}"
        )

        # download data
        extract_data_task = BashOperator(
            task_id=f"extract_{service}_tripdata",
            bash_command=f"wget {url} -O {filename}",
        )

        # load data into database
        upload_to_postgres_task = PythonOperator(
            task_id=f"upload_{service}_tripdata",
            python_callable=upload_to_postgres,
            op_kwargs={
                'filename': filename,
                'tablename': tablename,
                'schema': 'public'
            }
        )

        setup_local_ingestion >> extract_data_task >> upload_to_postgres_task

