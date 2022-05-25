import fastavro
import json
import time
import os

from io import BytesIO
from confluent_kafka import Producer


WRITE_PARTITION = int(os.getenv("WRITE_PARTITION"))
print("=== Started Producer ===")
producer_conf = {
    'bootstrap.servers': 'uat:9092', 
    'client.id': 'new_producer', 
}
producer = Producer(producer_conf)

test_string = {"h": "g"}


for i in range(1000): 
    with BytesIO() as stream:
        producer.produce(
            "autoscaler-test", 
            json.dumps(test_string),
            partition=WRITE_PARTITION, 
            headers={
                "item_type_name": "de_avro.DummyItem",
            }
        )
        producer.flush()
    time.sleep(1)

time.sleep(60*5)
