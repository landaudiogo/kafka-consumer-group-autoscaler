import sys
import asyncio
import time
import json
import functools

from config import IGNORE_EVENTS
from messaging.consumer import ConfluentKafkaConsumer
from messaging.serializer import SerializationException


class RowList(list): 
    def __init__(self, *rows): 
        self.byte_size = functools.reduce(
            lambda row, accum: accum+len(row['even_json']), 
            rows, 0
        )
        self.bytes_per_topic = {}
        super().__init__(*rows)

    def append(self, row, topic_name): 
        self.byte_size += len(row['event_json'])
        if not self.bytes_per_topic.get(topic_name): 
            self.bytes_per_topic[topic_name] = len(row['event_json'])
        else: 
            self.bytes_per_topic[topic_name] += len(row['event_json'])
        super().append(row)

    def pop(self, idx): 
        self.byte_size -= len(self[idx]['event_json'])
        return super().pop(idx)


class Timeline:
    def __init__(self): 
        self.start = time.time()
        self.chronology = [[0, 'Beginning']]
        self.coded_tstamps = {}

    def add(self, key, code=None):
        ctime = time.time()-self.start
        if code != None: 
            if self.coded_tstamps.get(code): 
                self.coded_tstamps[code].append([ctime, key])
            else: 
                self.coded_tstamps[code] = [[ctime, key]]
        self.chronology.append([ctime, key])
            
    def print(self):
        for tstamp in self.chronology: 
            print(f"{tstamp[0]},{tstamp[1]}")

    def print_code(self, code): 
        for tstamp in self.coded_tstamps[code]:
            print(f"{tstamp[0]},{tstamp[1]}")

    def current_tstamp(self): 
        return time.time() - self.start

    def last_tstamp(self, code): 
        lst = self.coded_tstamps.get(code)
        if lst: 
            return lst[-1][0]





class Batch:

    def __init__(self): 
        self.rows = []
        self.size = 0

    def append(self, row): 
        self.rows.append(row)
        self.size += len(row['event_json'])



class BatchList:


    def __init__(self, gcp_client, batch_size=5_000_000):
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
            }, msg.topic() 
        else:
            eprint("Message error")
            return None
    except Exception as e:
        eprint(e)
        eprint('=== Failed Deserialization, Sending to bytes bucket ===')
        await gcp_client.bytes_to_bucket(msg.value())
        # await asyncio.sleep(1)
        return None

async def pre_process(
    msg_list: list, 
    row_list,
    conf_consumer: ConfluentKafkaConsumer,
    gcp_client
) -> None:

    tasks = [ 
        asyncio.ensure_future(evt_to_row(record, conf_consumer, gcp_client))
        for record in msg_list
    ]
    msgs = await asyncio.gather(*tasks)
    
    for [row, topic_name] in msgs:
        if (row != None) and (row['event_type'] not in IGNORE_EVENTS):
            row_list.append(row, topic_name)



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
