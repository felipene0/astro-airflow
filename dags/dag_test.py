import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

API = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

@dag(
    dag_id='cityofny',
    schedule='@daily',
    start_date=datetime(2021,12,1),
    catchup=False
)
def main():

    # TODO Task Group 1
    with TaskGroup("transform") as transformer:

        # TODO Task 1
        @task(task_id="extract", retries=2)
        def extract():
            return requests.get(API).json()
        
        # TODO Task 2
        @task(task_id="transform")
        def transform(response):
            return response
        
        # TODO Depedencies
        processed_data = transform(extract())
    
    # TODO Task Group 2
    with TaskGroup("store") as stores:
        
        # TODO Task 1
        @task(task_id="store")
        def store(data):
            for record in data:
                logging.info(f"Crash date: {record['crash_date']}")

        store(processed_data)

main()

