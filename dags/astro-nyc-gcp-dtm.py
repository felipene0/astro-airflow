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

# The code snippet bellow is reponsible for fetching data from the NYC Open data API using Dynamic Task Mapping for two different boroughs, and then storing in GCP.

import requests
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from dateutil.relativedelta import relativedelta


API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={limit}&$offset={offset}&$where=crash_date>='{date_filter}'&borough={borough}"
BOROUGH_LIST = ['BROOKLYN', 'QUEENS']
DATE_FILTER = (datetime.now() - relativedelta(weeks=10)).strftime("%Y-%m-%d")
LIMIT = 50000

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

@dag(
    dag_id="astro-nyc-gcp-dtm",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
)
def main():

    @task(map_index_template="{{ borough }}")
    # @task
    def fetch_data(borough: str):
        offset = 0
        all_data = []

        # Use borough name in the task name
        # context = get_current_context()
        # context["borough"] = borough

        try:
            while True:
                    api_url = API.format(limit=LIMIT, offset=offset, date_filter=DATE_FILTER, borough=borough)
                    # response = requests.get(f"{API}?$limit={LIMIT}&$offset={offset}&$where=crash_date >= '{DATE_FILTER}'&$where=borough = '{}", timeout=20)
                    response = requests.get(api_url, timeout=20)
                    response.raise_for_status()

                    data = response.json()

                    logging.info(f"Fetched {len(data)} rows starting from offset {offset}")
                    all_data.extend(data)

                    # Break the loop if no more data is fetched
                    if len(data) < LIMIT:
                        break

                    offset += LIMIT
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data for {borough}: {e}")
            raise
        
        return all_data
    
    # Dynamically map the fetch_data task over the list of boroughs
    fetch_data.partial().expand(borough=BOROUGH_LIST)

main()