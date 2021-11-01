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
    while True: 
        if controller.state == 1: 
            controller.execute_s1()

            elapsed_time = controller.state_elapsed_time()
            for consumer in controller.consumer_list:
                if consumer.combined_speed > CONSUMER_CAPACITY:
                    controller.EXCEEDED_CAPACITY = True

            if (
                (elapsed_time > MAX_TIME_S1)
                or (len(controller.unassigned_partitions) > 0)
                or (controller.EXCEEDED_CAPACITY == True)
            ):
                controller.change_state(2)


        elif controller.state == 2:
            controller.execute_s2()
            controller.change_state(1)

        elif controller.state == 3:
            pass



if __name__ == '__main__': 
    main()
