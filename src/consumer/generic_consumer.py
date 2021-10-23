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
    CONSUMER_CONFIG, BATCH_BYTES, WAIT_TIME_SECS
)
from utils import (
    Timeline, RowList, process
)


timeline = Timeline()   # CODES 
                        # 0: new consumer iteration began
                        # 1: finished streaming rows 
                        # 2: bytes inserted per topic

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
                consume_time = timeline.current_timediff(code=0)
                list_batchlist = process(consumer.row_list)
                process_time = timeline.current_timediff(code=0) - consume_time
                with GCPClient() as gc: 
                    result = asyncio.run(gc.send_bq(list_batchlist))
                insert_time = timeline.current_timediff(code=0) - (process_time + consume_time)
                # consumer.commit(offsets=[
                    # nxt_offset
                    # for bl in list_batchlist 
                        # for nxt_offset in bl.tp_list_commit()
                # ])

                total_bytes = total_rows = 0
                for batchlist in list_batchlist:
                    total_bytes += batchlist.total_bytes
                    total_rows += batchlist.total_rows
                    items = list(batchlist.tp_offsets.items())
                    # items.sort()
                    # for k, ve in items:
                        # vs = batchlist.tp_start_offsets[k]
                        # print(k[0], k[1], vs, ve)
                    # print(total_bytes)
                # print()
                speed = total_bytes/timeline.current_timediff(code=0)
                print(
                    f"{total_bytes:,}\t"
                    f"{round(timeline.current_timediff(code=0),2)}\t"
                    f"{round(consume_time, 2)}\t"
                    f"{round(process_time, 2)}\t"
                    f"{round(insert_time, 2)}\t"
                    f"{int(speed):,}\t"
                    f"{total_rows}\t"
                    f"{len(list_batchlist)}"
                )

            consumer.consume_metadata()
            timeline.add('', code=0)
        else:
            consumer.consume(num_messages=1, timeout=0)

