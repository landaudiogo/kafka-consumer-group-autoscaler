import json
import os
import logging

MONITOR_CONSUMER_CONFIG = {
    'bootstrap.servers': "uat:9092", 
    'group.id': "data-engineering-autoscaler", 
    'auto.offset.reset': 'earliest',
}

CONTROLLER_CONSUMER_CONFIG = {
    'bootstrap.servers': "uat:9092", 
    'group.id': "data-engineering-autoscaler", 
    'auto.offset.reset': 'earliest',
}

ADMIN_CONFIG = {
    'bootstrap.servers': "uat:9092", 
}
CONTROLLER_PRODUCER_CONFIG = {
    'bootstrap.servers': 'uat:9092', 
    'client.id': 'controller-producer', 
}

MAX_TIME_S1 = 10
MAX_TIME_STATE_GM = 60*5
CONSUMER_CAPACITY = 133
ALGO_CAPACITY = 100
CONTROLLER_ENV = os.getenv("CONTROLLER_ENV")

LOGGER_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
GENERAL_LOGGER_LEVEL = os.getenv("GENERAL_LOGGER_LEVEL", default="INFO")
STATE_MACHINE_LOGGER_LEVEL = os.getenv("STATE_MACHINE_LOGGER_LEVEL", default=GENERAL_LOGGER_LEVEL)
if (GENERAL_LOGGER_LEVEL not in LOGGER_LEVELS):
    raise InvalidLoggerLevel(f"{GENERAL_LOGGER_LEVEL}")
if (STATE_MACHINE_LOGGER_LEVEL not in LOGGER_LEVELS):
    raise InvalidLoggerLevel(f"{STATE_MACHINE_LOGGER_LEVEL}")
GENERAL_LOGGER_LEVEL = getattr(logging, GENERAL_LOGGER_LEVEL)
STATE_MACHINE_LOGGER_LEVEL = getattr(logging, STATE_MACHINE_LOGGER_LEVEL)

logging.basicConfig(
    format=f'%(asctime)-26s%(levelname)-8s| [%(name)s] %(message)s', 
    level=GENERAL_LOGGER_LEVEL
)

with open("detopic_metadata.json", "r") as f:
    DETopicMetadata = json.load(f)
