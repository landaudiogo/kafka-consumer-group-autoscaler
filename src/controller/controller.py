import time
import json

from confluent_kafka import Consumer, TopicPartition
from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY
)
from utilities import Timeline
from dstructures import TopicPartitionConsumer, ConsumerList
from typing import Tuple



class Controller: 


    def __init__(self):
        self.monitor_consumer = Consumer(MONITOR_CONSUMER_CONFIG)
        _, next_off = self.monitor_consumer.get_watermark_offsets(
            TopicPartition(topic="data-engineering-monitor", partition=0)
        )
        last_off = next_off - 1
        self.monitor_consumer.assign([TopicPartition(
            topic="data-engineering-monitor", partition=0, offset=last_off
        )])
        while True:
            cstate = self.monitor_consumer.assignment()[0].offset
            if cstate == last_off:
                break
            time.sleep(0.01)

        self.controller_consumer = Consumer(CONTROLLER_CONSUMER_CONFIG)
        self.controller_consumer.assign([TopicPartition(
            topic="data-engineering-monitor", partition=0
        )])

        self.timeline = Timeline()
        self.state = None
        self.change_state(1)
        self.unassigned_partitions = []
        self.consumer_list = ConsumerList()
        self.map_partition_consumer = {}
        self.EXCEEDED_CAPACITY = False

    def get_monitor_last(self): 
        start_off, next_off = self.monitor_consumer.get_watermark_offsets(
            TopicPartition(topic="data-engineering-monitor", partition=0)
        )
        if start_off == next_off: 
            return None

        last_off = next_off - 1
        self.monitor_consumer.seek(TopicPartition(
            topic="data-engineering-monitor", partition=0, offset=last_off
        ))
        while True:
            msg = self.monitor_consumer.poll(timeout=0)
            if msg != None: 
                if msg.error() == None:
                    return msg.value()
            else: 
                time.sleep(0.01)


    def change_state(self, state):
        self.timeline.add(f'State Change to {state}', code=0)
        if state == 1: 
            self.EXCEEDED_CAPACITY = False
        self.state = state

    def execute_s1(self): 
        msg_string = self.get_monitor_last()
        partition_speeds = json.loads(msg_string)
        for topic_name, p_speeds in partition_speeds.items(): 
            for p_str, speed  in p_speeds.items():
                speed = min(CONSUMER_CAPACITY, speed)
                p_int = int(p_str)
                tp = TopicPartitionConsumer(topic_name, p_int)

                consumer = self.get_consumer(tp)
                if consumer == None:
                    tp.update_speed(speed)
                    self.unassigned_partitions.append(tp)
                else:
                    consumer.update_partition_speed(tp, speed)


    def execute_s2(self): 
        pass

    def execute_s3(self):
        pass
    
    def state_elapsed_time(self): 
        return self.timeline.current_timediff(code=0)

    def assign_partition_consumer(self, consumer, tp: TopicPartitionConsumer): 
        self.map_partition_consumer[tp] = consumer
        consumer.add_partition(tp)

    def get_consumer(self, tp: TopicPartitionConsumer):
        return self.map_partition_consumer.get(tp)
