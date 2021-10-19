import sys
import asyncio
import time
import json
import functools

from typing import List, Dict

from config import IGNORE_EVENTS, BATCH_BYTES
from messaging.consumer import ConfluentKafkaConsumer
from messaging.serializer import SerializationException
from confluent_kafka import TopicPartition

class Row: 


    def __init__(self, topic, partition, offset, payload, bq_table): 
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.payload = payload
        self.payload_size = len(json.dumps(payload))
        self.table = bq_table
        

class RowList(list): 


    def __init__(self, rows: List[Row] = []): 
        self.byte_size = functools.reduce(
            lambda row, accum: accum+row.payload_size, 
            rows, 0
        )
        self.tp_bytes = {}
        for row in rows: 
            if self.tp_bytes.get((row.topic, row.partition)) == None:
                self.tp_bytes[(row.topic, row.partition)] = row.payload_size
            else:
                self.tp_bytes[(row.topic, row.partition)] += row.payload_size
        super().__init__(*rows)

    def append(self, row: Row): 
        self.byte_size += row.payload_size
        if self.tp_bytes.get((row.topic, row.partition)) == None:
            self.tp_bytes[(row.topic, row.partition)] = row.payload_size
        else:
            self.tp_bytes[(row.topic, row.partition)] += row.payload_size
        super().append(row)

    def pop(self, idx: int): 
        self.byte_size -= self[idx].payload_size
        row = super().pop(idx)
        self.tp_bytes[(row.topic, row.partition)] -= row.payload_size
        return row

    def extend(self, rows: 'RowList'):
        self.byte_size += rows.byte_size
        for k, v in rows.tp_bytes.items(): 
            if self.tp_bytes.get(k) == None:
                self.tp_bytes[k] = v
            else: 
                self.tp_bytes[k] += v
        super().extend(rows)

    


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

    def current_timediff(self, code=None):
        if code == None: 
            return self.current_timestamp()
        return (self.current_tstamp() - self.last_tstamp(code) 
            if self.last_tstamp(code) != None
            else None
        )



class Batch(list):


    def __init__(self): 
        super().__init__()
        self.size = 0

    def append(self, row: Row): 
        super().append(row)
        self.size += row.payload_size

    def rows_payload(self):
        return [row.payload for row in self]



class BatchList(list):


    def __init__(self, table, weak_max_batch_size=5_000_000):
        super().__init__([Batch()])
        self.total_bytes = 0
        self.total_rows = 0
        self.weak_max_batch_size = weak_max_batch_size
        self.to_bucket = []
        self.table = table
        self.tp_offsets = {}
        self.tp_start_offsets = {}

    def add_row(self, row: Row): 
        if row.payload_size < 9_000_000:
            if ((self.last_bin().size + row.payload_size < self.weak_max_batch_size)
                or (self.last_bin().size == 0)
            ):
                self.last_bin().append(row)
            else:
                super().append(Batch())
                self.last_bin().append(row)
        else:
            self.to_bucket.append(row)
        self.total_bytes += row.payload_size
        self.total_rows += 1
        self.tp_offsets[(row.topic, row.partition)] = row.offset
        if self.tp_start_offsets.get((row.topic, row.partition)) == None:
            self.tp_start_offsets[(row.topic, row.partition)] = row.offset

    def last_bin(self):
        return self[-1]

    def tp_list_commit(self):
        return [
            TopicPartition(topic=k[0], partition=k[1], offset=v+1) 
            for k, v in self.tp_offsets.items()
        ]

def print_result(batch_list, times):
    print(f'''\n\nINSERTED {batch_list.num_events} EVENTS({batch_list.total_bytes:,} bytes)''')
    print(f'''PRE-PROCESSING => {round(times['proc'], 3)}s ({round((times['proc'])/(times['end'])*100, 3)}%)''')
    print(f'''INSERT => {round(times['end']-times['proc'], 3)}s ({round((times['end']-times['proc'])/(times['end'])*100, 3)}%)''')
    print(f'''TOTAL => {round(times['end'], 3)}s\n\n''')

def process(row_list: RowList) -> None:
    table_batchlist, bytes_batched = {}, 0
    while len(row_list) and bytes_batched < BATCH_BYTES: 
        row = row_list.pop(0)
        if table_batchlist.get(row.table) == None:
            table_batchlist[row.table] = BatchList(row.table)
        table_batchlist[row.table].add_row(row)
        bytes_batched += row.payload_size
    return list(table_batchlist.values())

def eprint(*args, **kwargs): 
    print(*args, file=sys.stderr, **kwargs)

def timer(func):
    functools.wraps(func)
    def new_function(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        end = time.time() - start
        return res 
    return new_function

