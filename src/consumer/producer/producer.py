import fastavro

from io import BytesIO
from confluent_kafka import Producer

TopicPartitionsSchema = {
    "name": "TopicPartition",
    "type": "record",
    "fields": [
        {"name": "topic_name", "type": "string"},
        {"name": "bq_table", "type": "string"},
        {
            "name": "topic_class", 
            "type": {
                "name": "topic_class",
                "type": "record",
                "fields": [
                    {"name": "module_path", "type": "string"},
                    {"name": "class_name", "type": "string"},
                ]
            }
        },
        {
            "name": "partitions", 
            "type": {
                "type": "array", 
                "items": {
                    "name": "partition",
                    "type": "int"
                }
            }
        },
        {
            "name": "ignore_events", 
            "type": {
                "type": "array", 
                "items": {
                    "name": "event_type",
                    "type": "string"
                }
            }
        },
    ]
}

DEControllerSchema = {
    "name": "DEControllerSchema",
    "type": "array",
    "items": TopicPartitionsSchema
}

# ControllerSchemaExample = [
    # {
        # "topic_name": "delivery_events_v6_topic",
        # "topic_class": {
            # "module_path": "contracts.delivery.delivery_events_v6",
            # "class_name": "DeliveryEventsV6Topic"
        # },
        # "partitions": [0, 1,2],
        # "bq_table": "delivery_events_temp",
        # "ignore_events": []
    # }, 
    # {
        # "topic_name": "delivery_events_v7_topic",
        # "topic_class": {
            # "module_path": "contracts.delivery.delivery_events_v7",
            # "class_name": "DeliveryEventsV7Topic"
        # },
        # "partitions": [3, 4, 5],
        # "bq_table": "delivery_events_temp",
        # "ignore_events": []
    # }
# ]

# [SMSPhysicalStockUpdatedTopic(), ShippingPricesEventsTopic(),
# CarrierAccountServiceEventsTopic(), ExtEventsTopic(), Ext3plEventsTopic(),
# Ext3plCommandsTopic(), Ext3plEventsV2Topic()], 
ControllerSchemaExample2 = [
    {
        "topic_name": "sms_physical_stock_updated_topic",
        "topic_class": {
            "module_path": "contracts.sms.sms_physical_stock_updated_events",
            "class_name": "SMSPhysicalStockUpdatedTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "sms_physical_stock_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "shipping_prices_events_topic",
        "topic_class": {
            "module_path": "contracts.shipping_prices_and_cost.shipping_prices",
            "class_name": "ShippingPricesEventsTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "brand_shipping_prices_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "carrier_account_service_events_topic",
        "topic_class": {
            "module_path": "contracts.shipping_prices_and_cost.carrier_account_service_events",
            "class_name": "CarrierAccountServiceEventsTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "carrier_account_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "3pl_external_events",
        "topic_class": {
            "module_path": "contracts.ext.topic",
            "class_name": "ExtEventsTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "ext_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "ext_3pl_events_topic",
        "topic_class": {
            "module_path": "contracts.ext_3pl.ext_3pl_events",
            "class_name": "Ext3plEventsTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "ext_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "ext_3pl_commands_topic",
        "topic_class": {
            "module_path": "contracts.ext_3pl.ext_3pl_commands",
            "class_name": "Ext3plCommandsTopic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "ext_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "ext_3pl_events_v2_topic",
        "topic_class": {
            "module_path": "contracts.ext_3pl.ext_3pl_v2_events",
            "class_name": "Ext3plEventsV2Topic"
        },
        "partitions": [i for i in range(16)],
        "bq_table": "ext_events_temp",
        "ignore_events": []
    }, 
    {
        "topic_name": "delivery_events_v7_topic",
        "topic_class": {
            "module_path": "contracts.delivery.delivery_events_v7",
            "class_name": "DeliveryEventsV7Topic"
        },
        "partitions": [3, 4],
        "bq_table": "delivery_events_temp",
        "ignore_events": []
    }
]


parsed_schema = fastavro.parse_schema(DEControllerSchema)

print("=== Started Producer ===")
producer_conf = {
    'bootstrap.servers': '52.213.38.208:9092', 
    'client.id': 'new_producer', 
}
producer = Producer(producer_conf)

with BytesIO() as stream:
    fastavro.schemaless_writer(stream, parsed_schema, ControllerSchemaExample2)
    producer.produce(
        "data-engineering-controller", stream.getvalue(),
        partition=1, 
        headers={
            "serializer": "generic_consumer.DEControllerSchema",
            "event_type": "StartConsumingCommand"
        }
    )
    producer.flush()

