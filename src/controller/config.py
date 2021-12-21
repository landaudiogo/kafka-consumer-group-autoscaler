import json

MONITOR_CONSUMER_CONFIG = {
    'bootstrap.servers': "uat:9092", 
    'group.id': "data-engineering-controller", 
    'auto.offset.reset': 'earliest',
}

CONTROLLER_CONSUMER_CONFIG = {
    'bootstrap.servers': "uat:9092", 
    'group.id': "data-engineering-controller", 
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
CONSUMER_CAPACITY = 200
ALGO_CAPACITY = 100

with open("detopic_metadata.json", "r") as f:
    DETopicMetadata = json.load(f)
