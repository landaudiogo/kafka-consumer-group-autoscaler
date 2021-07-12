import json 
import csv
import calendar

from google.cloud import (
    storage,
    bigquery as bq
)
from google.oauth2 import service_account
from pandas_gbq import gbq

from config import BQ_CLIENT_CONFIG, GCP_BUCKET

import asyncio 
from functools import wraps, partial
import time

def use_bucket(client, rows): 
    rows = [row.values() for row in rows]
    with open('GFG.csv', 'w', newline='') as f:
        write = csv.writer(f, quoting=csv.QUOTE_ALL)
        write.writerows(rows)
    big_query_insert_result = None
    bucket_name = GCP_BUCKET
    source_file_name = "GFG.csv"
    destination_blob_name = "{}.csv".format(calendar.timegm(time.gmtime()))
    credentials = service_account.Credentials.from_service_account_file(
        BQ_CLIENT_CONFIG['credentials_path'], scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    storage_client = storage.Client(
        credentials=credentials
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    table_id = f"{BQ_CLIENT_CONFIG['project']}.{BQ_CLIENT_CONFIG['dataset']}.{BQ_CLIENT_CONFIG['table']}"
    job_config = bq.LoadJobConfig(
        schema=[
            bq.SchemaField("event_type", "STRING"),
            bq.SchemaField("event_json", "STRING"),
            bq.SchemaField("stream_timestamp", "INTEGER"),
            bq.SchemaField("stream_timestamp_hour", "TIMESTAMP"),
            bq.SchemaField("stream_timestamp_date", "DATE"),
        ],
        # The source format defaults to CSV, so the line below is optional.
        source_format=bq.SourceFormat.CSV,
    )
    uri = "gs://{}/{}".format(bucket_name, destination_blob_name)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) # Make an API request.
    res = load_job.result()  # Waits for the job to complete.
    blob.delete()
    storage_client.close()

def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run

async def stream_rows(client, batch, loop=None):
    try: 
        table_ref = (client
            .dataset(BQ_CLIENT_CONFIG['dataset'])
            .table(BQ_CLIENT_CONFIG['table'])
        )
        table = client.get_table(table_ref)  # API request
        result = await client.insert_rows(table, batch.rows, loop=loop)  # API request
        if result:
            rows = [
                batch.rows[error['index']]
                for error in result
            ]
            use_bucket(client, rows)
    except Exception as e:
        coros = [
            insert_row(client, row, table, loop=loop)
            for row in batch.rows
        ]
        await asyncio.gather(*coros)
        
async def insert_row(client, row, table, loop):
    try: 
        result = await client.insert_rows(table, [row], loop=loop)  # API request
        if result:
            use_bucket(client, [row])
    except Exception as e:
        print(e)
        use_bucket(client, [row])
        return False


class BQClient: 

    def __init__(self): 
        self.credentials = service_account.Credentials.from_service_account_file(
            BQ_CLIENT_CONFIG['credentials_path'], scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        pass

    def __enter__(self):
        self.client = bq.Client(
            credentials=self.credentials, project=self.credentials.project_id
        )
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()

    def stream_rows(self,  batch_list):
        try:
            loop = asyncio.get_event_loop()
            self.client.insert_rows = async_wrap(self.client.insert_rows)
            tasks = [
                asyncio.ensure_future(stream_rows(self.client, batch, loop=loop))
                for batch in batch_list.batches
            ]
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            print('=== EXCEPTION ===')
            raise e


class StorageClient:
    def __init__(self): 
        self.credentials = service_account.Credentials.from_service_account_file(
            BQ_CLIENT_CONFIG['credentials_path'], scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        pass

    def __enter__(self):
        self.client = storage.Client(
            credentials=self.credentials
        )
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()

    def stream_rows(self, batch_list):
        try:
            loop = asyncio.get_event_loop()
            self.client.insert_rows = async_wrap(self.client.insert_rows)
            tasks = [
                asyncio.ensure_future(stream_rows(self.client, batch, loop=loop))
                for batch in batch_list.batches
            ]
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            print('=== EXCEPTION ===')
            raise e


