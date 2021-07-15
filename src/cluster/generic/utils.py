import sys
import asyncio
import time
import json

from config import IGNORE_EVENTS
from messaging.consumer import ConfluentKafkaConsumer
from messaging.serializer import SerializationException

class Batch:


    def __init__(self): 
        self.rows = []
        self.size = 0

    def append(self, row): 
        self.rows.append(row)
        self.size += len(row['event_json'])



class BatchList:


    def __init__(self, gcp_client, batch_size=9_000_000):
        self.batches = [Batch()]
        self.total_bytes = 0
        self.num_events = 0
        self.batch_size = batch_size
        self.gcp_client = gcp_client

    async def add_row(self, row): 
        if len(row['event_json']) < 9_000_000:
            if self.batches[-1].size + len(row['event_json']) < self.batch_size:
                self.batches[-1].append(row)
            else:
                self.batches.append(Batch())
                self.batches[-1].append(row)
        else:
            eprint('=== String too big ===')
            await self.gcp_client.use_bucket([row])
        self.total_bytes += len(row['event_json'])
        self.num_events += 1


async def evt_to_row(msg, conf_consumer, gcp_client):
    try: 
        if not msg.error():
            return {
                'event_type': (
                    dict(msg.headers())['item_type_name']
                        .decode()
                        .split('.')[-1]
                ),
                'event_json': json.dumps(
                    conf_consumer.deserialize_msg_value(msg)._asdict()
                ),
                'stream_timestamp': (
                    time.time_ns()
                ), 
                'stream_timestamp_hour': time.strftime(
                    "%Y-%m-%d %H:00:00", 
                    time.gmtime()
                ),
                'stream_timestamp_date': time.strftime(
                    "%Y-%m-%d", 
                    time.gmtime()
                ),
            } 
        else:
            eprint("Message error")
    except Exception as e:
        eprint(e)
        eprint('=== Failed Deserialization, Sending to bytes bucket ===')
        await gcp_client.bytes_to_bucket(msg.value())

async def pre_process(
    msg_list: list, 
    conf_consumer: ConfluentKafkaConsumer,
    gcp_client
) -> list:
    return [
        row
        for row in [
            await evt_to_row(evt, conf_consumer, gcp_client)
            for evt in msg_list
        ]
        if (row != None) 
            and (row['event_type'] not in IGNORE_EVENTS)
    ]

async def split_batches(rows: list, gcp_client) -> BatchList:
    batch_list = BatchList(gcp_client=gcp_client)
    coros = [
        batch_list.add_row(rows.pop(0))
        for i in range(len(rows))
    ]
    await asyncio.gather(*coros)
    return batch_list

def print_result(batch_list, times):
    print(f'''\n\nINSERTED {batch_list.num_events} EVENTS({batch_list.total_bytes:,} bytes)''')
    print(f'''PRE-PROCESSING => {round(times['proc'], 3)}s ({round((times['proc'])/(times['end'])*100, 3)}%)''')
    print(f'''INSERT => {round(times['end']-times['proc'], 3)}s ({round((times['end']-times['proc'])/(times['end'])*100, 3)}%)''')
    print(f'''TOTAL => {round(times['end'], 3)}s\n\n''')


def eprint(*args, **kwargs): 
    print(*args, file=sys.stderr, **kwargs)
