
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

MAX_TIME_S1 = 30
CONSUMER_CAPACITY = 2_000_000
