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

import requests
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
LIMIT = 50000
DATE_FILTER = (datetime.now() - timedelta(weeks=1)).strftime("%Y-%m-%d")

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

@dag(
    dag_id='cityofny',
    schedule='@daily',
    start_date=datetime(2021,12,1),
    catchup=False
)
def main():

    # Task Group 1
    with TaskGroup("transform") as transformer:

        # Task 1
        @task(task_id="extract", retries=2)
        def extract():
            offset = 0
            all_data = []

            try:
                while True:
                    response = requests.get(f"{API}?$limit={LIMIT}&$offset={offset}&$where=crash_date >= '{DATE_FILTER}'", timeout=20)
                    response.raise_for_status()

                    data = response.json()

                    logging.info(f"Fetched {len(data)} rows starting from offset {offset}")
                    all_data.extend(data)

                    # Break the loop if no more data is fetched
                    if len(data) < LIMIT:
                        break

                    offset += LIMIT
                    
                return all_data
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"API request failed: {e}")
        
        # Task 2
        @task(task_id="transform_dataframe")
        def transform_dataframe(data):

            if not isinstance(data, list):
                raise ValueError("Unexpected data format")
            
            df = pd.DataFrame(data)

            if df.empty:
                logging.info("No data to process")
                return []
            
            logging.info(f"Transformed data into DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

            if 'crash_date' in df.columns:
                return df.to_dict(orient="records")

        @task(task_id="processing_data")
        def processing_data(data):
            
            return 
        # Depedencies
        processed_data = transform_dataframe(extract())
        
    # Task Group 2
    with TaskGroup("store") as stores:
        
        # Task 1
        @task(task_id="store")
        def store(data):
            for record in data:
                logging.info(f"Crash date: {record.get('crash_date', 'N/A')}")
            logging.info(f"Stored a total of {len(data)} records.")

        store(processed_data)

main()