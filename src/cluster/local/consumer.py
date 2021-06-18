import os
from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from struct import unpack

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer, Consumer
from confluent_kafka.schema_registry.avro import (
    _schema_loads,
    _ContextStringIO
)

from schema_api import get_parsed_schema


topic_name = os.getenv('TOPIC_NAME')

schema_registry_conf = {
    'url': 'http://172.22.0.4:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
parsed_schema_id = schema_registry_client.get_latest_version(f'{topic_name}-value').schema_id
parsed_schema = get_parsed_schema(f'{topic_name}-value')

conf = {
    'bootstrap.servers': 'broker:29092', 
    'group.id': f'{topic_name}', 
    'auto.offset.reset': 'earliest',
#    'plugin.library.paths': 'monitoring-interceptor'
}
consumer = Consumer(conf)
consumer.subscribe([topic_name])

# while True: 
msg = consumer.poll()
with _ContextStringIO(msg.value()) as payload:
    magic, schema_id = unpack('>bI', payload.read(5))
    value = schemaless_reader(payload, parsed_schema)
print(os.getenv('TOPIC_NAME'))

consumer.close()

