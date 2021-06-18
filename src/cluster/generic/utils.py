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

    def __init__(self, batch_size=9_000_000):
        self.batches = [Batch()]
        self.total_bytes = 0
        self.num_events = 0
        self.batch_size=batch_size

    def add_row(self, row): 
        if len(row['event_json']) < 9_000_000:
            if self.batches[-1].size + len(row['event_json']) < self.batch_size:
                self.batches[-1].append(row)
            else:
                self.batches.append(Batch())
                self.batches[-1].append(row)
            self.total_bytes += len(row['event_json'])
            self.num_events += 1
        else:
            print('event_json is too big to insert into bigquery via http')
            pass


def evt_to_row(evt, consumer):
    try: 
        return {
            'event_type': (
                dict(evt.headers())['item_type_name']
                    .decode()
                    .split('.')[-1]
            ),
            'event_json': json.dumps(
                consumer.deserialize_msg_value(evt)._asdict()
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
    except SerializationException as se:
        # could not deserialize message
        return None


def pre_process(msg_list: list, consumer: ConfluentKafkaConsumer) -> list:
    return [
        row
        for row in [
            evt_to_row(evt, consumer)
            for evt in msg_list
        ]
        if (row != None) 
            and (row['event_type'] not in IGNORE_EVENTS)
    ]

def split_batches(rows: list) -> BatchList:
    batch_list = BatchList()
    for i in range(len(rows)):
        batch_list.add_row(rows.pop(0))
    return batch_list

def print_result(batch_list, times):
    print(f'''\n\nINSERTED {batch_list.num_events} EVENTS({batch_list.total_bytes:,} bytes)''')
    print(f'''PRE-PROCESSING => {round(times['proc'], 2)}s ({round((times['proc'])/(times['end'])*100, 2)}%)''')
    print(f'''INSERT => {round(times['end']-times['proc'], 2)}s ({round((times['end']-times['proc'])/(times['end'])*100, 2)}%)''')
    print(f'''TOTAL => {round(times['end'], 2)}s\n\n''')
