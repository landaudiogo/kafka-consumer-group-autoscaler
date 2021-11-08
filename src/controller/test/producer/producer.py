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
    "delivery_events": {
        0: 4,
        1: 10,
        2: 120, 
        5: 199,
        6: 10,
    }, 
    "delivery_events_2": {
        0: 1, 
        2: 5,
        3: 6,
    },
    "delivery_events_7": {
        0: 4,
    },
}

msg = json.dumps(partition_speeds)

producer.produce("data-engineering-monitor", msg)
producer.flush()
