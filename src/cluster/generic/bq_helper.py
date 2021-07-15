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
from utils import eprint

import asyncio 
from functools import wraps, partial
import time


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
        table_ref = (client.bq_session
            .dataset(BQ_CLIENT_CONFIG['dataset'])
            .table(BQ_CLIENT_CONFIG['table'])
        )
        table = client.bq_session.get_table(table_ref)
        result = await client.bq_session.insert_rows(
            table, batch.rows, loop=loop
        )
        if result:
            rows = [
                batch.rows[error['index']]
                for error in result
            ]
            await client.use_bucket(rows)
    except Exception as e:
        eprint(e)
        eprint('=== Failed Batch insert ===')
        coros = [
            insert_row(client, row, table, loop=loop)
            for row in batch.rows
        ]
        await asyncio.gather(*coros)
        
async def insert_row(client, row, table, loop):
    try: 
        result = await client.bq_session.insert_rows(table, [row], loop=loop)
        if result:
            await client.use_bucket([row])
    except Exception as e:
        eprint(e)
        eprint('=== Failed single row insert ===')
        await client.use_bucket([row])


class GCPClient: 

    def __init__(self): 
        self.credentials = service_account.Credentials.from_service_account_file(
            BQ_CLIENT_CONFIG['credentials_path'], scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        pass

    def __enter__(self):
        self.bq_session = bq.Client(
            credentials=self.credentials, project=self.credentials.project_id
        )
        self.storage_session = storage.Client(
            credentials=self.credentials
        )
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.bq_session.close()
        self.storage_session.close()

    def stream_rows(self,  batch_list, loop=None):
        try:
            if not loop:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            self.bq_session.insert_rows = async_wrap(self.bq_session.insert_rows)
            tasks = [
                asyncio.ensure_future(stream_rows(self, batch, loop=loop))
                for batch in batch_list.batches
            ]
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            eprint('=== EXCEPTION ===')
            raise e

    async def use_bucket(self, rows): 
        """Used to send a row to a Bucket in case it cannot be inserted into the
        table using the API.
        
        In the scenario it cannot be inserted into the table even using this method,
        then the bucket will not be deleted.
        
        """

        rows = [row.values() for row in rows]
        source_file_name = destination_blob_name = f'{uuid.uuid4().hex}.csv'
        with open(source_file_name, 'w', newline='') as f:
            write = csv.writer(f)
            write.writerows(rows)
        try:
            bucket = self.storage_session.bucket(GCP_BUCKET)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename = async_wrap(blob.upload_from_filename)
            await blob.upload_from_filename(source_file_name)

            table_id = f"{BQ_CLIENT_CONFIG['project']}.{BQ_CLIENT_CONFIG['dataset']}.{BQ_CLIENT_CONFIG['table']}"
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

    async def bytes_to_bucket(self, failed_msg_bytes):
        """Inserts bytes into a bucket in google cloud platform

        """

        source_file_name = destination_blob_name = f'{uuid.uuid4().hex}.bytes'
        with open(source_file_name, 'wb') as f:
            f.write(failed_msg_bytes)
        try: 
            bucket = self.storage_session.bucket(GCP_BUCKET)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)
        except Exception as e:
            eprint(e) 
            eprint("=== Failed to insert malformed evt into bytes bucket ===")
        finally:
            os.remove(source_file_name)
