from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer, SerializingProducer

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
test5_latest = schema_registry_client.get_latest_version('test5-value')
test5_avro_serializer = AvroSerializer(schema_registry_client, test5_latest.schema.schema_str)
print(test5_latest.schema.schema_str)


conf = {
    'bootstrap.servers': 'broker:29092', 
    'client.id': 'test_producer', 
    'value.serializer': test5_avro_serializer
}
producer = SerializingProducer(conf)
producer.produce('test5', value={'name': 'Diogo', 'id': 10})
producer.flush()


# producer = Producer(conf)

# producer.produce('test5', value='hello')
# producer.flush()
