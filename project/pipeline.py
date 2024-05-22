import pandas as pd
import requests
from io import BytesIO
import sqlite3
import os

def download_csv_from_url(url, headers=None):
    df = pd.read_csv(url)
    return df.dropna()

def load_to_database(df, table_name):    
    # Connect to the SQLite database or create a new one if it doesn't exist
    db_file = '../data/data.sqlite'
    if not os.path.exists(db_file):
        # Create an empty database file
        open(db_file, 'w').close()

    conn = sqlite3.connect(database=db_file)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def run_pipline():
    url_cycle_counts = "https://data.smartdublin.ie/dataset/d26ce6c0-2e1c-4b72-8fbd-cb9f9cbbc118/resource/3dc32c0c-b2cd-40a1-af09-434b6f4a007d/download/2021-dublin-city-cycle-counts-31122021.csv"
    url_air_quality = "https://data.smartdublin.ie/dataset/8cfd5686-4d69-41d9-9fd4-e128f0b811b5/resource/ae6a399e-6e02-414b-b21d-24e1060ee362/download/dublin-city-council-no-no2-2012.csv"

    df_air_quality = download_csv_from_url(url_air_quality)
    df_cycle_counts = download_csv_from_url(url_cycle_counts)

    load_to_database(df_air_quality, 'air_quality')
    load_to_database(df_cycle_counts, 'cycle_counts')