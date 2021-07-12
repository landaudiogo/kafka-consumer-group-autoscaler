import os
import time
import json
from io import BytesIO
import gc
import tracemalloc

from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer
from messaging.consumer import ConfluentKafkaConsumer
from bq_helper import BQClient
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
print("STARTING v1.0.5")

with ConfluentKafkaConsumer(CONSUMER_CONFIG, [TOPIC], None) as c:
    consumer = c.consumer
    while True: 
        msg_list = consumer.consume(timeout=5, num_messages=50)
        if not msg_list:
            continue
        try:
            times = {'start': time.time()}
            msg_list = pre_process(msg_list, c) 
            batch_list = split_batches(msg_list) 
            times['proc'] = time.time()-times['start']
            with BQClient() as bq_client:
                bq_client.stream_rows(batch_list) 
            times['end'] = time.time() - times['start']
            print_result(batch_list, times)
            gc.collect()
        except Exception as e:
            raise e 

print('closing')
