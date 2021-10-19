import os
import time
import json
from io import BytesIO
import gc
import tracemalloc
import asyncio
import fastavro

from bq_helper import GCPClient
from confluent_kafka import TopicPartition
from huub_consumer import (
    DEConsumer, DETopic, DETopicPartition, DETopicDict, ChangeStateEvent
)
from config import (
    CONSUMER_CONFIG, TOPIC, IGNORE_EVENTS,
    BATCH_BYTES, WAIT_TIME_SECS
)
from utils import (
    print_result, Timeline, RowList, process
)


timeline = Timeline()   # CODES 
                        # 0: new consumer iteration began
                        # 1: finished streaming rows 
                        # 2: bytes inserted per topic

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

test = {
    "name": "topic_partition", 
    "type": "record", 
    "fields": [
        {"name": "topic", "type": "string"},
        {"name": "partition", "type": "int"},
    ]
}


DEControllerSchema = {
    "name": "DEControllerSchema",
    "type": "array",
    "items": TopicPartitionsSchema
}

ControllerSchemaExample = [
    {
        "topic_name": "delivery_events_v6_topic",
        "topic_class": {
            "module_path": "contracts.delivery.delivery_events_v6",
            "class_name": "DeliveryEventsV6Topic"
        },
        "partitions": [0, 1,2],
        "bq_table": "delivery_events_temp",
        "ignore_events": []
    }, {
        "topic_name": "delivery_events_v7_topic",
        "topic_class": {
            "module_path": "contracts.delivery.delivery_events_v7",
            "class_name": "DeliveryEventsV7Topic"
        },
        "partitions": [3, 4, 5],
        "bq_table": "delivery_events_temp",
        "ignore_events": []
    }
]

timeline = Timeline()
with DEConsumer(CONSUMER_CONFIG) as consumer:
    consumer.consume_metadata()

    print("=== STARTING CONSUMPTION ===")
    timeline.add('', code=0)
    while True:
        if (
            (consumer.row_list.byte_size > BATCH_BYTES)
            or (timeline.current_timediff(code=0) > WAIT_TIME_SECS)
        ):
            if consumer.row_list:
                list_batchlist = process(consumer.row_list)
                with GCPClient() as gc: 
                    result = asyncio.run(gc.send_bq(list_batchlist))
                # consumer.commit(offsets=[
                    # nxt_offset
                    # for bl in list_batchlist 
                        # for nxt_offset in bl.tp_list_commit()
                # ])

                total_bytes = 0
                for batchlist in list_batchlist:
                    total_bytes += batchlist.total_bytes
                    items = list(batchlist.tp_offsets.items())
                    # items.sort()
                    # for k, ve in items:
                        # vs = batchlist.tp_start_offsets[k]
                        # print(k[0], k[1], vs, ve)
                    # print(total_bytes)
                # print()
                print(f"{total_bytes},{timeline.current_timediff(code=0)},{len(list_batchlist)}")

            consumer.consume_metadata()
            timeline.add('', code=0)
        else:
            consumer.consume(num_messages=100_000, timeout=0)
