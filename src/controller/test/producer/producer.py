import fastavro
import json

from io import BytesIO
from confluent_kafka import Producer

print("=== Started Producer ===")
producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'new_producer', 
}
producer = Producer(producer_conf)

partition_speeds = {
    "delivery_events_v6_topic": {
        0: 9,
        1: 0,
        2: 1, 
        5: 6,
        6: 1,
        7: 56, 
        8: 31,
    }, 
    "delivery_events_v7_topic": {
        0: 20,
        1: 33,
        2: 81,
        4: 77,
        7: 71,
        8: 10,
        9: 11,
    },
}

msg = json.dumps(partition_speeds)

producer.produce("data-engineering-monitor", msg)
producer.flush()
