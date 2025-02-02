import logging
import os
import io
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

file = "crash_data.parquet"
# Environment variables
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_OUTPUT_FOLDER = os.getenv('GCS_OUTPUT_FOLDER')
GBQ_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

@dag(
    dag_id="astro-nyc-gcp-read",
    schedule_interval="@daily",
    start_date=datetime(2025,1,1),
    catchup= False
)
def main():
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    @task(task_id="fetch_and_process")
    def fetch_and_process() -> pd.DataFrame:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)

        blobs = bucket.list_blobs()
        dfs = []

        for blob in blobs:
            if not blob.name.endswith(".parquet"):
                continue
            logging.info(f"Processing blob: {blob.name}")

            try:
                bytes_data = blob.download_as_bytes()
                logging.info(f"Downloaded {len(bytes_data)} from blob {blob.name}")
                data_io = io.BytesIO(bytes_data)
                df = pd.read_parquet(data_io, engine="pyarrow")
                logging.info(f"DataFrame shape {df.shape} from blob {blob.name}")
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error processing blob {blob.name}: {e}")
        
        if not dfs:
            logging.warning(f"No parquet files found in {GCS_OUTPUT_FOLDER}")
            return pd.DataFrame()
        
        final_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"Final concatenated DataFrame with shape: {final_df.shape}")
        return final_df
    
    @task(task_id="preparing")
    def preparing(df: pd.DataFrame) -> pd.DataFrame:
        df_streets = df.dropna(subset=["on_street_name"])
        df_streets = df_streets[["borough", "on_street_name"]].drop_duplicates()

        return df_streets
    
    @task(task_id="big_query_insert")
    def big_query_insert(df: pd.DataFrame):
        df.to_gbq(destination_table='TestDataInsert.TestTable',
                  chunksize=1000,
                  project_id=GBQ_PROJECT,
                  if_exists="replace"
                  )        
            
    fetch_and_process = fetch_and_process()
    preparing = preparing(fetch_and_process)
    inserting = big_query_insert(preparing)

    start >> fetch_and_process >> preparing >> inserting >> end

main()