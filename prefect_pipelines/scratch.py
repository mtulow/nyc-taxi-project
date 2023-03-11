import pandas as pd
from prefect.blocks.system import DateTime



def main():

    data_time_block = DateTime.load("example-date")
    print(data_time_block)
    print(data_time_block.data)


    # yellow_df = pd.read_parquet('')