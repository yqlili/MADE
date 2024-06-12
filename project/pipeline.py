import pandas as pd
import sqlite3
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time


def download_csv_from_url(url):

    # Define the download directory relative to the current folder
    download_dir = "data/"
    # Get the target directory to store data from the environment variable
    target_folder_path = os.environ.get('TARGET_DIR')

    # Concatenate the current folder path and download directory
    full_download_dir = os.path.join(target_folder_path, download_dir)

    # Set up Chrome options
    chrome_options = Options()
    
    # Set the download directory
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": full_download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    })

    # Create a new instance of the Chrome driver
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Open the webpagg
        driver.get(url)

       # Wait for the button to be clickable
        button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-toggle="dropdown"][title="Download"]'))
        )

        time.sleep(5) 
        
        # Click the button
        button.click()

        # Wait for the download link to be clickable
        download_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.downloadcsv.ng-binding.ng-scope'))
        )

        # Click the download link
        download_link.click()

        # Wait for the download to complete
        time.sleep(10)  

    finally:
        # Stop the service
        driver.quit()


def read_emission_csv():
    # Define the download directory relative to the current folder
    download_dir = "data/"
    # Get the target directory to store data from the environment variable
    target_folder_path = os.environ.get('TARGET_DIR')
    data_folder_path = os.path.join(target_folder_path, download_dir)
    

    # List all files in the directory
    files = os.listdir(data_folder_path)

    # Search for the first file starting with "Emissions"
    emissions_file = None
    for file in files:
        if file.startswith('Emissions'):
            emissions_file = file
            break

    # If "Emissions" file is found, read it
    if emissions_file:
        file_path = os.path.join(data_folder_path, emissions_file)
        df = pd.read_csv(file_path)
        print("DataFrame from the first 'Emissions' file:", df)
        return df
    else:
        print("No 'Emissions' file found in the directory.")

def read_vehicle_csv():
    # Define the download directory relative to the current folder
    download_dir = "data/"
    # Get the target directory to store data from the environment variable
    target_folder_path = os.environ.get('TARGET_DIR')
    data_folder_path = os.path.join(target_folder_path, download_dir)
    

    # List all files in the directory
    files = os.listdir(data_folder_path)

    # Search for the first file starting with "Emissions"
    vehicle_file = None
    for file in files:
        if file.startswith('Vehicle'):
            vehicle_file = file
            break

    # If "Emissions" file is found, read it
    if vehicle_file:
        file_path = os.path.join(data_folder_path, vehicle_file)
        df = pd.read_csv(file_path)
        print("DataFrame from the first 'Vehicle' file:", df)
        return df
    else:
        print("No 'Vehicle' file found in the directory.")


def load_to_database(df, table_name):    
    # Connect to the SQLite database or create a new one if it doesn't exist
    db_name = 'data.sqlite'

    # Define the download directory relative to the current folder
    download_dir = "data/"
    # Get the target directory to store data from the environment variable
    target_folder_path = os.environ.get('TARGET_DIR')
    db_file = os.path.join(target_folder_path, download_dir, db_name)
    
    if not os.path.exists(db_file):
        # Create an empty database file
        open(db_file, 'w').close()

    conn = sqlite3.connect(database=db_file)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def run_pipline():

    url_emissions = "https://opendata.cbs.nl/statline/#/CBS/en/dataset/85347ENG/table"
    url_vehicle = "https://opendata.cbs.nl/statline/#/CBS/en/dataset/84651ENG/table"
    
    download_csv_from_url(url_emissions)
    download_csv_from_url(url_vehicle)

    df_emissions = read_emission_csv()
    load_to_database(df_emissions, "Emissions")

    df_vehicle = read_vehicle_csv()
    load_to_database(df_vehicle, "Vehicle")

if __name__ == "__main__":
    run_pipline()
