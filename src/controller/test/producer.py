import fastavro
import json

from io import BytesIO
from confluent_kafka import Producer

parsed_schema = fastavro.parse_schema(DEControllerSchema)

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
        3: 100, 
        4: 0,
    }
}

msg = json.dumps(partition_speeds)

producer.produce("data-engineering-monitor", msg)
producer.flush()
