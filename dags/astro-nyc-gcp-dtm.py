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
DATE_FILTER = (datetime.now() - relativedelta(weeks=30)).strftime("%Y-%m-%d")
LIMIT = 50000
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_OUTPUT_FOLDER = os.getenv('GCS_OUTPUT_FOLDER')

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
        
        # Only pass files that are bigger than 1MB for any reason...
        @task.branch(task_id='branching')
        def checker(file_path: list):
            valid_files = []
            for file in file_path:
                size = os.path.getsize(file)
                
                logging.info(f"File {file} has size of {size} bytes")
                if size < 1000000:
                    logging.info(f"Removing file {file}")
                    os.remove(file)
                else:
                    valid_files.append(file)

            if valid_files:
                logging.info('There are no files above 1MB to be ingested')
                return ['end']

            return valid_files

        @task
        def combine_files(file_path: list) -> pd.DataFrame:
            combined_df = pd.DataFrame()

            for file in file_path:
                df = pd.read_parquet(file)
                combined_df = pd.concat([combined_df, df], ignore_index=True)

            combined_local_file = "/tmp/crash_data.parquet"
            combined_df.to_parquet(combined_local_file, engine="pyarrow", index=False)
            logging.info(f"Saved combined Dataframe to {combined_local_file}")

            return combined_local_file
        
        # Group Depedencies
        file_paths = fetch_data_dtm.expand(borough=BOROUGH_LIST)
        checker = checker(file_paths)
        combined_data = combine_files(checker)
        
        file_paths >> checker 
        checker >> end # Branch to skip upload data
        checker >> combined_data

        
        return combined_data
            
    @task
    def upload_gcs(file_path: list):
        client = storage.Client()

        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"{GCS_OUTPUT_FOLDER}crash_data.parquet")
        blob.upload_from_filename(file_path)

        logging.info(f"Uploaded {file_path} to GCS at {GCS_OUTPUT_FOLDER} as {blob}")
        
        # Clean up local file
        os.remove(file_path)

    final_data = data_processing()
    upload = upload_gcs(final_data)
    
    # TODO step 'start' is pointing directly to combine_files, not sure why
    start >> final_data >> upload >> end
main()