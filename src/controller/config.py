import json

MONITOR_CONSUMER_CONFIG = {
    'bootstrap.servers': "broker:29092", 
    'group.id': "data-engineering-controller", 
    'auto.offset.reset': 'earliest',
}

CONTROLLER_CONSUMER_CONFIG = {
    'bootstrap.servers': "broker:29092", 
    'group.id': "data-engineering-controller", 
    'auto.offset.reset': 'earliest',
}

MAX_TIME_S1 = 300 # 5 minutes
CONSUMER_CAPACITY = 200
ALGO_CAPACITY = 100

with open("detopic_metadata.json", "r") as f:
    DETopicMetadata = json.load(f)
