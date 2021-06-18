import json 

from io import BytesIO
from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from huub_schemas import DeliveryEventV6AvroSchema


schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
variant = schema_registry_client.get_latest_version(
    'variant-value'
)
print(variant.schema.schema_str)
print(variant.schema.references)
named_schemas = {}
for ref in variant.schema.references: 
    collection = schema_registry_client.get_version(
        ref['subject'], 
        ref['version']
    )
    parse_schema(
        json.loads(collection.schema.schema_str), 
        named_schemas
    )

variant_parsed = parse_schema(
    json.loads(variant.schema.schema_str),
    named_schemas
)

