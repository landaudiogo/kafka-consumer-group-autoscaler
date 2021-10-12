from confluent_kafka import Consumer, KafkaError
import json

consumer_config = {
    'bootstrap.servers': 'broker:29092',
    'group.id': 'analyse-consumer-11', 
    'auto.offset.reset': 'earliest',
    'enable.partition.eof': True
}

consumer = Consumer(consumer_config)
consumer.subscribe(['monitor-speed', 'producer-speed'])

monitor_logs = []
producer_logs = []

NUM_EOFS = 0

while NUM_EOFS != 2:
    msg = consumer.poll(timeout = 0.1)
    if msg == None:
        continue
    error = msg.error()
    if error == None:
        if(msg.topic() == 'monitor-speed'):
            monitor_logs.append([float(msg.value().decode()), msg.timestamp()[1]])
        elif(msg.topic() == 'producer-speed'):
            producer_logs.append([float(msg.value().decode()), msg.timestamp()[1]])
    elif (error == KafkaError._PARTITION_EOF):
        NUM_EOFS += 1

with open('/usr/src/data/monitor.log', 'w') as f:
    json.dump(monitor_logs, f)
with open('/usr/src/data/producer.log', 'w') as f:
    json.dump(producer_logs, f)
