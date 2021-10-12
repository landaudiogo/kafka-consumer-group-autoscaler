import os
import time
import json
from io import BytesIO
import gc
import tracemalloc
import asyncio

from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader
from confluent_kafka import Consumer
from messaging.consumer import ConfluentKafkaConsumer
from contracts.delivery.delivery_events_v7 import DeliveryEventsV7Topic
from contracts.delivery.delivery_events_v8 import DeliveryEventsV8Topic
from contracts.sms.sms_physical_stock_updated_events import SMSPhysicalStockUpdatedTopic
from contracts.shipping_prices_and_cost.shipping_prices import ShippingPricesEventsTopic
from contracts.shipping_prices_and_cost.carrier_account_service_events import CarrierAccountServiceEventsTopic
from contracts.ext.topic import ExtEventsTopic
from contracts.ext_3pl.ext_3pl_events import Ext3plEventsTopic
from contracts.ext_3pl.ext_3pl_commands import Ext3plCommandsTopic
from contracts.ext_3pl.ext_3pl_v2_events import Ext3plEventsV2Topic
from bq_helper import GCPClient
from config import (
    CONSUMER_CONFIG, 
    TOPIC, 
    IGNORE_EVENTS
)
from utils import (
    pre_process, 
    split_batches, 
    print_result, 
    Timeline,
    RowList,
)
# print("STARTING")

with ConfluentKafkaConsumer(
    CONSUMER_CONFIG, 
    [DeliveryEventsV7Topic(), DeliveryEventsV8Topic()], #DeliveryEventsV8Topic(), DeliveryEventsV7Topic(), TOPIC], 
    None
) as c:
# with ConfluentKafkaConsumer(
    # CONSUMER_CONFIG, 
    # [SMSPhysicalStockUpdatedTopic(), ShippingPricesEventsTopic(),
     # CarrierAccountServiceEventsTopic(), ExtEventsTopic(), Ext3plEventsTopic(),
     # Ext3plCommandsTopic(), Ext3plEventsV2Topic()], 
    # None
# ) as c:
# with ConfluentKafkaConsumer(
    # CONSUMER_CONFIG, 
    # [SMSPhysicalStockUpdatedTopic(), ShippingPricesEventsTopic(),
     # CarrierAccountServiceEventsTopic(), ExtEventsTopic(), Ext3plEventsTopic(),
     # Ext3plCommandsTopic(), Ext3plEventsV2Topic(), DeliveryEventsV8Topic(),
     # DeliveryEventsV7Topic()], #DeliveryEventsV8Topic(), DeliveryEventsV7Topic(), TOPIC], 
    # None
# ) as c:
    consumer = c.consumer
    total_bytes = 0 
    time.sleep(10)
    timeline = Timeline()
    row_list = RowList()
    for i in range(20):
        timeline.add("", code=0)
        while(True):
            msg_list = consumer.consume(num_messages=1_000_000, timeout = 0)
            time_diff = timeline.current_tstamp() - timeline.last_tstamp(0)
            if msg_list or (time_diff > 0.1):
                with GCPClient() as gcp_client:
                    asyncio.run(pre_process(msg_list, row_list, c, gcp_client))
                    # gcp_client.stream_rows(batch_list) 
                print(time_diff)
                if ((row_list.byte_size > 5_000_000)
                    or (time_diff > 0.1)
                ):
                    print(f"=== {i} ===")
                    print(f"total {row_list.byte_size}")
                    if(row_list != []):
                        row_num = len(row_list)
                        # timeline.add(f'{row_list.byte_size},{len(row_list)}')
                        batch_list = asyncio.run(split_batches(row_list, gcp_client))
                        gcp_client.stream_rows(batch_list)
                        timeline.add(
                            f'{timeline.last_tstamp(0)},{batch_list.total_bytes},{row_num}', code=1
                        )
                        bytes_per_topic = [
                            f"{key}->{num_bytes}"
                            for key, num_bytes in row_list.bytes_per_topic.items()
                        ]
                        timeline.add(','.join(bytes_per_topic), code=2)
                        # print_result(batch_list, times)
                        consumer.commit()
                    break
            elif not msg_list: 
                time.sleep(0.1)
    timeline.print_code(1)
    timeline.print_code(2)
    time.sleep(86400)
# print('closing')
