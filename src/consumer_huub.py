from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from huub_schemas import DeliveryEventV6AvroSchema

consumer_conf = {
    'bootstrap.servers': '52.213.38.208:9092', 
    'group.id': 'landau_test_consumer6', 
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
    'name': 'delivery_event',
    'type': 'record',
    'fields': DeliveryEventV6AvroSchema
}
parsed_schema = parse_schema(schema)
consumer = Consumer(consumer_conf)
consumer.subscribe(['delivery_events_v6_topic'])

# producer 

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
delivery_schema = schema_registry_client.get_schema(52)
# delivery_schema = schema_registry_client.get_latest_version('delivery_event-value')
print(delivery_schema.schema_str)
delivery_serializer = AvroSerializer(schema_registry_client, delivery_schema.schema_str)

producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'delivery_producer', 
    'value.serializer': delivery_serializer, 
    'plugin.library.paths': 'monitoring-interceptor'
}
producer = SerializingProducer(producer_conf)

while True:
    msg = consumer.poll()
    with BytesIO(msg.value()) as buff:
        value = schemaless_reader(buff, parsed_schema)
    print(value)
    break
    producer.produce('delivery_events_v6_topic', value=value)
    producer.flush()
