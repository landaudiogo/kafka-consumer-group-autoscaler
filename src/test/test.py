from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
test_flatten_schema = schema_registry_client.get_latest_version('test_flatten-value')
test_flatten_serializer = AvroSerializer(schema_registry_client, test_flatten_schema.schema.schema_str)

producer_conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'flatten_producer', 
    'value.serializer': test_flatten_serializer, 
}
producer = SerializingProducer(producer_conf)
msg = {
    'id': 1, 
    'flatten_attr': {
        'flatten_1': "test", 
        'flatten_2': "nao facas isso"
    }
}
print(msg)

producer.produce('test_flatten', value=msg)
producer.flush()
