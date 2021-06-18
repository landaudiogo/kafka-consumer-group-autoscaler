import json 
from io import BytesIO

from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

def get_parsed_schema(subject_name, version=None, named_schemas=None):
    r"""Get a parsed schema and it's references using a single function call

    Arguments
    =========
    :subject: subject name to get from the schema-registry. To follow Kafka
        Connect conventions, the schema to fetch from the schema-registry might
        follow a naming convention which goes {topic-name}-{[key|value]},
        depending on whether it is the record's key or value to deserialize. As
        an example, if a message from the sms_physical_stock_updated_topic topic
        is consumed, then if we are trying to deserialize it's key, then the
        schema to fetch from the schema registry would be under the subject
        'sms_physical_stock_updated_topic-key'. If it's the value of the message
        the code is going to deserialize, then the schema to fetch is the 
        'sms_physical_stock_updated_topic-value'.
    :version: this is the version of the subject to be fetched from the
        database. In the case this value is None, then the latest version if
        fetched from the database.
    :named_schemas: dictionary that is used to add any schemas that are
        referenced. This is what allows references to be used in the schema
        registry. Check avro documentation to better understand what this
        parameter is used for. 
        ('https://fastavro.readthedocs.io/en/latest/schema.html#fastavro-schema')
    """
    if not version:
        schema = schema_registry_client.get_latest_version(
            subject_name
        ).schema       
    else:
        schema = schema_registry_client.get_version(
            subject_name, version
        ).schema       

    for ref in schema.references: 
        get_parsed_schema(
            ref['subject'], 
            version=ref['version'],
            named_schemas=named_schemas
        )
    return parse_schema(json.loads(schema.schema_str), named_schemas)
