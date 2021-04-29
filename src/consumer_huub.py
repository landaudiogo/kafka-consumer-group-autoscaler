from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

consumer_conf = {
    'bootstrap.servers': '52.213.38.208:9092', 
    'group.id': 'landau_test_consumer2', 
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
    'name': 'SMS3PLAdjustPhysicalStockUpdatedEvent',
    'type': 'record',
    'fields': sms_fields
}
parsed_schema = parse_schema(schema)
consumer = Consumer(consumer_conf)
consumer.subscribe(['sms_physical_stock_updated_topic'])

# producer 

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
sms_latest = schema_registry_client.get_latest_version('sms_physical_stock_updated_topic-value')
sms_avro_serializer = AvroSerializer(schema_registry_client, sms_latest.schema.schema_str)

producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'test_producer', 
    'value.serializer': sms_avro_serializer
}
producer = SerializingProducer(producer_conf)

while True:
    msg = consumer.poll()
    with BytesIO(msg.value()) as buff:
        value = schemaless_reader(buff, parsed_schema)
    producer.produce('sms_physical_stock_updated_topic', value=value)
    producer.flush()
    print(value)
