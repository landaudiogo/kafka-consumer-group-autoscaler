from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer

schema_registry_conf = {
    'url': 'http://localhost:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
sms_schema = schema_registry_client.get_latest_version('sms_physical_stock_updated_topic-value')
sms_deserializer = AvroDeserializer(schema_registry_client, sms_schema.schema.schema_str)
print(sms_schema.schema.schema_str)

conf = {
    'bootstrap.servers': '52.213.38.208:9092', 
    'group.id': 'landau_test_consumer0', 
    'value.deserializer': sms_deserializer, 
    'auto.offset.reset': 'earliest'
}
consumer = DeserializingConsumer(conf)
consumer.subscribe(['sms_physical_stock_updated_topic'])
msg = consumer.poll()
print(msg.value())
