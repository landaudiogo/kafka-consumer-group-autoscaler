import os
import importlib
import json


# BIGQUERY
BQ_CLIENT = {
    'prod': {
        'credentials_path': '/usr/src/app/bq_prod.json', 
        'project': 'huub-dwh-prod', 
        'dataset': 'event_sourcing', 
        'table': os.getenv('BQ_TABLE'), 
    },
    'uat': {
        'credentials_path': '/usr/src/app/bq_uat.json', 
        'project': 'huub-dwh', 
        'dataset': 'event_sourcing', 
        'table': os.getenv('BQ_TABLE'), 
    }
}
WRITE_ENV = os.getenv('WRITE_ENV')
BQ_CLIENT_CONFIG = BQ_CLIENT[f'{WRITE_ENV}']
GCP_BUCKET = f'lost_events_{WRITE_ENV}'
BATCH_BYTES = int(os.getenv('BATCH_BYTES'))
WAIT_TIME_SECS = float(os.getenv('WAIT_TIME_SECS'))

# KAFKA
KAFKA = {
    'prod': ['54.76.46.203:9092', '18.202.250.11:9092'], # ,'54.171.156.36:9092'],
    'uat': ['52.213.38.208:9092']
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
    'queued.max.messages.kbytes': 1_000, 
    'max.partition.fetch.bytes': 900_000,
    # 'plugin.library.paths': 'monitoring-interceptor'
}
METADATA_CONF = {
    "bootstrap.servers": ','.join(KAFKA["uat"]),
    'group.id': os.getenv('GROUP_ID'), 
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}


# CONSUMER
module_str = os.getenv('IMPORT_PATH')
object_str = os.getenv('IMPORT_OBJECT')
module = importlib.import_module(module_str)
TOPIC = getattr(module, object_str)()
IGNORE_EVENTS = json.loads(os.getenv('IGNORE_EVENTS'))
