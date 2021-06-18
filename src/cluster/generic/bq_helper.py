import json 

from google.cloud import bigquery as bq 
from google.oauth2 import service_account
from pandas_gbq import gbq

from config import BQ_CLIENT_CONFIG

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
            # insert into postgres
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
            pass
            # insert into postgres
    except Exception as e:
        # insert into postgres
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

