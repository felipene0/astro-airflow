"""

- Imports
- Connections & Variables
- Datasets
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instantiation

"""

# The code snippet bellow is reponsible for fetching data from the NYC Open data API using Dynamic Task Mapping for two different boroughs, and then storing in GCS.

import os
import requests
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from dateutil.relativedelta import relativedelta
from google.cloud import storage

API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={limit}&$offset={offset}&$where=crash_date>='{date_filter}'&borough={borough}"
BOROUGH_LIST = ['BROOKLYN', 'QUEENS']
DATE_FILTER = (datetime.now() - relativedelta(months=12)).strftime("%Y-%m-%d")
LIMIT = 50000
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_OUTPUT_FOLDER = os.getenv('GCS_OUTPUT_FOLDER')
FILE_SIZE_THRESHOLD = 500000  # 500KB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

@dag(
    dag_id="astro-nyc-gcp-dtm",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
)
def main():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    @task_group(group_id="data_processing")
    def data_processing():
        
        @task(map_index_template="{{ borough }}")
        def fetch_data_dtm(borough: str):
            offset = 0
            all_data = []
            
            try:
                while True:
                    api_url = API.format(limit=LIMIT, offset=offset, date_filter=DATE_FILTER, borough=borough)
                    response = requests.get(api_url, timeout=20)
                    response.raise_for_status()

                    data = response.json()

                    logging.info(f"Fetched {len(data)} rows starting from offset {offset}")
                    all_data.extend(data)

                    # Break the loop if no more data is fetched
                    if len(data) < 1:
                        break

                    offset += LIMIT
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch data for {borough}: {e}")
                raise

            df = pd.DataFrame(all_data)
            local_file = f"/tmp/{borough}_crash_data.parquet"
            df.to_parquet(local_file, engine="pyarrow", index=False)
            logging.info(f"Saved {borough} into a DataFrame with shape {df.shape} in {local_file}")
            return local_file
        
        # Only pass files that are bigger than 500KB for any reason...
        @task.branch(task_id='branching')
        def checker(file_paths: list) -> list:
            valid_files = []
            for file in file_paths:
                try:
                    size = os.path.getsize(file)
                    logging.info(f"File {file} has size of {size} bytes")

                    if size < FILE_SIZE_THRESHOLD:
                        logging.info(f"Removing file {file}")
                        os.remove(file)
                    else:
                        valid_files.append(file)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

            if not valid_files:
                logging.info('No files above 500kb to be ingested')
                return "end"
            
            logging.info("Branching to combine_files.")
            return "data_processing.combine_files"

        @task(task_id="combine_files")
        def combine_files(file_path: list) -> pd.DataFrame:
            combined_df = pd.DataFrame()

            for file in file_path:
                try:
                    df = pd.read_parquet(file)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                except Exception as e:
                    logging.error(f"Erroe reading file {file}: {e}")

            combined_local_file = "/tmp/crash_data.parquet"
            combined_df.to_parquet(combined_local_file, engine="pyarrow", index=False)
            logging.info(f"Saved combined Dataframe to {combined_local_file}")
            return combined_local_file

        # Group Depedencies
        file_paths = fetch_data_dtm.expand(borough=BOROUGH_LIST)
        valid_files = checker(file_paths)
        combined_data = combine_files(valid_files)
        
        valid_files >> [combined_data, end]

        return combined_data
            
    @task
    def upload_gcs(file_path: list):
        try:
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(f"{GCS_OUTPUT_FOLDER}crash_data.parquet")
            blob.upload_from_filename(file_path)
            logging.info(f"Uploaded {file_path} to GCS at {GCS_OUTPUT_FOLDER} as {blob}")
        except Exception as e:
            logging.error(f"Error uploading file to GCS: {e}")
            
        # Clean up local file
        os.remove(file_path)

    final_data = data_processing()
    upload = upload_gcs(final_data)
    
    # TODO step 'start' is pointing directly to combine_files, not sure why
    start >> final_data >> upload >> end
main()