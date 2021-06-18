import os
import time
import json
from io import BytesIO
import gc

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
print("STARTING")

with ConfluentKafkaConsumer(CONSUMER_CONFIG, [TOPIC], None) as c:
    consumer = c.consumer
    while True: 
        msg_list = consumer.consume(num_messages=50, timeout=0)
        if not msg_list:
            continue
        try:
            msg_list, times = pre_process(msg_list, c), {'start': time.time()}
            batch_list, times['proc'] = split_batches(msg_list), time.time()-times['start']
            with BQClient() as bq_client:
                bq_client.stream_rows(batch_list) 
            times['end'] = time.time() - times['start']
            print_result(batch_list, times)
            gc.collect()
        except Exception as e:
            raise e 



