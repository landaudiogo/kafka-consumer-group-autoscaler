import os
import importlib
import json
import re


DEPLOYMENT_NAME = open("/etc/podinfo/pod_name", "r").read().replace("\n", "")
POD_ID = None
if DEPLOYMENT_NAME:
    POD_ID = int(re.search(r"^\w+-\w+-(\w+).*$", DEPLOYMENT_NAME).group(1))
else:
    POD_ID = 1
print(POD_ID)

# DEConsumer
BATCH_BYTES = int(os.getenv('BATCH_BYTES'))
WAIT_TIME_SECS = float(os.getenv('WAIT_TIME_SECS'))


# BIGQUERY
BQ_CLIENT = {
    'prod': {
        'credentials_path': '/usr/src/app/bq_prod.json', 
        'project': 'huub-dwh-prod', 
        'dataset': 'event_sourcing', 
    },
    'uat': {
        'credentials_path': '/usr/src/app/bq_uat.json', 
        'project': 'huub-dwh', 
        'dataset': 'event_sourcing', 
    }
}
WRITE_ENV = os.getenv('WRITE_ENV')
BQ_CLIENT_CONFIG = BQ_CLIENT[f'{WRITE_ENV}']
GCP_BUCKET = f'lost_events_{WRITE_ENV}'


# KAFKA
KAFKA = {
    'prod': ['prod1:9092', 'prod2:9092'], 
    'uat': ['uat:9092']
}
BROKERS = KAFKA[os.getenv('CONSUME_ENV')]
CONSUMER_CONFIG = {
    'bootstrap.servers': ','.join(BROKERS), 
    'group.id': os.getenv('GROUP_ID'), 
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,

    'fetch.min.bytes': 1, 
    'fetch.wait.max.ms': 500,
    'max.in.flight': 10,
    'fetch.max.bytes': 900_000,
    'message.max.bytes': 900_000,
    'queued.max.messages.kbytes': 10_000, 
    'max.partition.fetch.bytes': 900_000,
    # 'plugin.library.paths': 'monitoring-interceptor'
}
METADATA_CONF = {
    "bootstrap.servers": ','.join(KAFKA["uat"]),
    'group.id': os.getenv('GROUP_ID'), 
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}
METADATA_PRODUCER = {
    "bootstrap.servers": ','.join(KAFKA["uat"]),
    "client.id": f"de-consumer-{POD_ID if POD_ID != None else 1}"
}


