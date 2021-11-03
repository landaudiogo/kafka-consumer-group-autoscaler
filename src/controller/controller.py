import time
import json

from typing import Tuple
from confluent_kafka import Consumer, TopicPartition
from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY
)
from utilities import Timeline
from dstructures import TopicPartitionConsumer, ConsumerList
from state_machine import (
    StateMachine, StateSentinel, StateReassignAlgorithm, StateGroupManagement
)



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


        s1 = StateSentinel(self)
        s2 = StateReassignAlgorithm(self, approximation_algorithm="mwf")
        s3 = StateGroupManagement(self)
        states = [
            ("s1", s1), 
            ("s2", s2), 
            ("s3", s3),
        ]
        transitions = [
            ("s1", "s2", s1.time_up),
            ("s1", "s2", s1.full_bin),
            ("s1", "s2", s1.any_unassigned),
            ("s2", "s1", s2.finished_approximation_algorithm)
        ]
        self.state_machine = StateMachine(
            self, states=states, transitions=transitions
        )
        self.state_machine.set_initial("s1")

        self.unassigned_partitions = []
        self.consumer_list = ConsumerList()

    def get_last_monitor_record(self): 
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
                    return json.loads(msg.value())
            else: 
                time.sleep(0.01)

    def run(self): 
        while True:
            self.state_machine.execute()
