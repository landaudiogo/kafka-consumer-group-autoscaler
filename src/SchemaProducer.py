from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry_conf = {
    'url': 'http://localhost:8081'
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
test5_latest = schema_registry_client.get_latest_version('test5-value')
test5_avro_serializer = AvroSerializer(schema_registry_client, test5_latest.schema.schema_str)


