from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from struct import pack
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, _MAGIC_BYTE
from confluent_kafka.schema_registry.avro import (
    AvroSerializer, 
    _schema_loads,
    _ContextStringIO
)

from huub_schemas import DeliveryEventV6AvroSchema
from schema_api import get_parsed_schema

production = {
    1: '18.202.250.11',
    2: '54.171.156.36',
    3: '54.76.46.203',    
}
uat = '52.213.38.208'

consumer_conf = {
    'bootstrap.servers': f'{production[1]}:9092', 
    'group.id': 'delivery_test_landau4', 
    'auto.offset.reset': 'earliest'
}
sms_fields = [
    {'name': 'timestamp', 'type': 'long'},
    {'name': 'process_reference', 'type': ['null', 'string']},
    {'name': 'variant_id', 'type': 'int'},
    {'name': 'warehouse_id', 'type': 'int'},
    {'name': 'warehouse_name', 'type': 'string'},
    {'name': 'huubclient_id', 'type': 'int'},
    {'name': 'ean', 'type': 'string'},
    {'name': 'reference', 'type': 'string'},
    {'name': 'sales_channel_type', 'type': 'string'},
    {'name': 'delta', 'type': 'int'},
    {'name': 'stock_after_adjust', 'type': 'int'},
]

schema = {
    'name': 'delivery_schema',
    'type': 'record',
    'fields': DeliveryEventV6AvroSchema
}
old_parsed = parse_schema(schema)
consumer = Consumer(consumer_conf)
consumer.subscribe(['delivery_events_v6_topic'])

# producer 

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
new_parsed = get_parsed_schema('delivery_event-value')
schema_id = schema_registry_client.get_latest_version('delivery_event-value').schema_id

producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'new_producer', 
#    'value.serializer': parsed_delivery_schema, 
#    'plugin.library.paths': 'monitoring-interceptor'
}
producer = Producer(producer_conf)

i = 0 
while True:
    msg = consumer.poll()
    with BytesIO(msg.value()) as buff:
        value = schemaless_reader(buff, old_parsed)
    with _ContextStringIO() as fo:
        # Write the magic byte and schema ID in network byte order (big endian)
        fo.write(pack('>bI', _MAGIC_BYTE, schema_id))
        # write the record to the rest of the buffer
        schemaless_writer(fo, new_parsed, value)
        producer.produce('delivery_event', value=fo.getvalue())
        producer.flush()
    print('delivery')
    i+=1
consumer.close()

