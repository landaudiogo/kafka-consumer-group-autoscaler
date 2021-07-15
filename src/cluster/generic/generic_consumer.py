import os
import time
import json
from io import BytesIO
import gc
import tracemalloc
import asyncio

from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer
from messaging.consumer import ConfluentKafkaConsumer
from bq_helper import GCPClient
from config import (
    CONSUMER_CONFIG, 
    TOPIC, 
    IGNORE_EVENTS
)
from utils import (
    pre_process, 
    split_batches, 
    print_result 
)
print("STARTING")

with ConfluentKafkaConsumer(CONSUMER_CONFIG, [TOPIC], None) as c:
    consumer = c.consumer
    while True: 
        msg_list = consumer.consume(timeout=1, num_messages=50)
        if not msg_list:
            continue
        try:
            times = {'start': time.time()}
            with GCPClient() as gcp_client:
                msg_list = asyncio.run(pre_process(msg_list, c, gcp_client))
                batch_list = asyncio.run(split_batches(msg_list, gcp_client))
                times['proc'] = time.time()-times['start']
                gcp_client.stream_rows(batch_list) 
            times['end'] = time.time() - times['start']
            print_result(batch_list, times)
            gc.collect()
            consumer.commit()
        except Exception as e:
            raise e 

print('closing')
