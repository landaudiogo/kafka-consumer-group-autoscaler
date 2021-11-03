import time
import signal
import sys
import json

from confluent_kafka import Consumer, TopicPartition, OFFSET_INVALID
from config import MAX_TIME_S1, CONSUMER_CAPACITY
from controller import Controller
from dstructures import (
    DataConsumer, TopicPartitionConsumer
)


def signal_handler(sig, frame):
    print('Exiting Safely')
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

def main(): 
    controller = Controller()
    controller.run()


if __name__ == '__main__': 
    main()
