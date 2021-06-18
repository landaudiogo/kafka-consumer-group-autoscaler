from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from huub_schemas import DeliveryEventV6AvroSchema

print('FIRST TEST PRINT')

production = {
    1: '18.202.250.11',
    2: '54.171.156.36',
    3: '54.76.46.203',    
}
uat = '52.213.38.208'

consumer_conf = {
    'bootstrap.servers': f'{uat}:9092', 
    'group.id': 'landau_sms_3', 
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
    'name': 'sms_physical_stock_updated_topic',
    'type': 'record',
    'fields': sms_fields
}
parsed_schema = parse_schema(schema)
consumer = Consumer(consumer_conf)
consumer.subscribe(['sms_physical_stock_updated_topic'])

# while True:
msg = consumer.poll()
with BytesIO(msg.value()) as buff:
    value = schemaless_reader(buff, parsed_schema)
print(value)
consumer.close()


# while True:
    # pass
