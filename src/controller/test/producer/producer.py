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
        0: 7,
        1: 8,
        2: 11, 
        5: 72,
        6: 12,
        7: 13, 
        8: 80,
    }, 
    "delivery_events_v7_topic": {
        0: 20,
        1: 3,
        2: 1,
        4: 70,
        7: 14,
        8: 3,
        9: 4,
    },
}

msg = json.dumps(partition_speeds)

producer.produce("data-engineering-monitor", msg)
producer.flush()
