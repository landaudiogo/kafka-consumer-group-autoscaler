import os
import json 
import csv
import calendar
import uuid

from google.cloud import (
    storage,
    bigquery as bq
)
from google.oauth2 import service_account
from pandas_gbq import gbq

from config import BQ_CLIENT_CONFIG, GCP_BUCKET
from utils import eprint, BatchList

import asyncio 
from functools import wraps, partial
import time
from typing import List


def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


class GCPClient: 

    def __init__(self): 
        self.credentials = service_account.Credentials.from_service_account_file(
            BQ_CLIENT_CONFIG['credentials_path'], scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        self.bq_session = None
        self.storage_session = None

    def __enter__(self):
        self.bq_session = bq.Client(
            credentials=self.credentials, project=self.credentials.project_id
        )
        self.bq_session.insert_rows = async_wrap(self.bq_session.insert_rows)

        self.storage_session = storage.Client(
            credentials=self.credentials
        )
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.bq_session.close()
        self.storage_session.close()

    async def send_bq(self, list_batchlist: List[BatchList]): 
        tasks = [
            asyncio.ensure_future(self.send_table(batchlist)) for batchlist in list_batchlist
        ]
        await asyncio.gather(*tasks)

    async def send_table(self, batchlist: BatchList):
        table = self.bq_session.get_table(
            self.bq_session.dataset(BQ_CLIENT_CONFIG['dataset']).table(batchlist.table) 
        )
        tasks = [
            asyncio.ensure_future(self.stream_batch(batch, table)) for batch in batchlist
        ]
        tasks.append(
            asyncio.ensure_future(self.use_bucket(batchlist.to_bucket.rows_payload(), batchlist.table))
        )
        await asyncio.gather(*tasks)

    async def stream_batch(self, batch, table):
        rows = batch.rows_payload()
        try: 
            result = await self.bq_session.insert_rows(table, rows)
            if result:
                rows = [batch.rows[error['index']] for error in result]
                # await self.use_bucket(rows)
        except Exception as e: 
            eprint(e)
            tasks = [
                self.bq_session.insert_rows(table, [row])
                for row in rows
            ]
            await asyncio.gather(*tasks)



    async def use_bucket(self, batch, bq_table_string): 
        """Used to send rows to a Bucket in case it cannot be inserted into the
        table using the API.
        
        In the scenario it cannot be inserted into the table even using this method,
        then the bucket will not be deleted.
        
        """

        if batch == []: 
            return 

        rows = [row.payload.values() for row in batch]
        source_file_name = destination_blob_name = f'{time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}-{uuid.uuid4().hex}.csv'
        with open(source_file_name, 'w', newline='') as f:
            write = csv.writer(f)
            write.writerows(rows)
        try:
            bucket = self.storage_session.bucket(GCP_BUCKET)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename = async_wrap(blob.upload_from_filename)
            await blob.upload_from_filename(source_file_name)

            table_id = f"{BQ_CLIENT_CONFIG['project']}.{BQ_CLIENT_CONFIG['dataset']}.{bq_table_string}"
            job_config = bq.LoadJobConfig(
                schema=[
                    bq.SchemaField("event_type", "STRING"),
                    bq.SchemaField("event_json", "STRING"),
                    bq.SchemaField("stream_timestamp", "INTEGER"),
                    bq.SchemaField("stream_timestamp_hour", "TIMESTAMP"),
                    bq.SchemaField("stream_timestamp_date", "DATE"),
                ],
                source_format=bq.SourceFormat.CSV,
            )
            uri = "gs://{}/{}".format(GCP_BUCKET, destination_blob_name)
            load_job = self.bq_session.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result = async_wrap(load_job.result)
            res = await load_job.result()
            if not res.error_result:
                blob.delete()
        except Exception as e:
            eprint(e)
            eprint('=== Failed using bucket ===')
            raise e
        finally:
            os.remove(source_file_name)
