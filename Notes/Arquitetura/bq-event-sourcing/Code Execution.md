# Code Organization

## Useful Links
> Official Confluent Kafka documentation for the python module: 
> https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer






## Execution

This repository leverages the django module to run the kafka consumer code.

### Django start up

Teh root directory contains a Dockerfile which describes the container environment.

At the end of this file there is a CMD call, which on container startup will run the following command: 
```
python manage.py kafka_consumer --env prod
```

manage.py is the file that django uses to determine the list of commands it has available to it. In this case it will be looking for a command called `kafka_consumer`.

Because this command does not belong to the native options django has avaiable, this command has to be set in the `delivery_to_bigquery/management/commands` directory. 

There, the python module `kafka_consumer.py` contains a `Command` class which also contains the `handle()` function. This function is called when the aforementioned CMD is executed at the end of the Dockerfile.

### Creating the Consumer

When starting up the command, there are a few attributes set in the command that help the Kafka Consumer understand it's concept.

the following attributes are set on instance creation `__init__`: 
- `topics`: 
```
self.topics = [ settings.CONSUMER_TOPICS['DELIVERY_EVENTS_V4_TOPIC'] ]
# This would be equivalent to doing 
self.topics = [ DeliveryEventsV6Topic() ]
```
- `config`: defines the consumer configurations for Kafka. 
```python
{
	"bootstrap.servers": KAFKA_SERVERS,
	"fetch.wait.max.ms": 1000,
	'group.id': cfg.GROUP_ID['consumer_delivery'], 
	'default.topic.config': {'auto.offset.reset': 'latest'},
	'enable.auto.commit': False
}
```
- `poll_timeout`:

When the kafka_consumer command is run, the `handle` function is called.

It begins with a context_manager, which creates a `ConfluentKafkaConsumer` with the following properties set on the `__init__()` method. 

```
# The None argument provides the possibility of adding a logger
with ConfluentKafkaConsumer(self.config, self.topics, None) as consumer
```
Not only does the above code create an instance of the called class, it also runs the `__enter__` method defined in the class. This last method is used to create a native consumer from the **confluent_kafka** module. 

from the `__enter__` method: 
```python
self.consumer = Consumer(self.conf)
self.consumer.subscribe([topic.name() for topic in self.topics], on_assign=self.__consumer_assignment_report)
```
The first line of the previous code, creates the Consumer with the configuration dictionary given in the `__init__` method, whereas the second defines the topics the consumer subscribes to. In the scope of the bigquery_event_sourcing repository, it only subscribes to the `DeliveryEventV6Topic()` instance.

### Consuming the Messages

Posterior to creating the Consumer, we enter a for cycle that iterates 100 000 times.
In each iteration, we use the consumer object previously created by the context manager `consumer`, to start polling for messages.

```python
msg = consumer.poll(self.poll_timeout)
```

within the poll method previously called, the method `poll_msg(timeout)` is called. 

```python
def poll_msg(self, timeout): 
	msg = self.consumer.poll(timeout=timeout)
```
this function calls the native poll function from the Consumer class in the confluent_kafka module.
`timeout`: Maximum time to block waiting for message, event or callback.
returns a Message obejct native to confluent_kafka.

on successfull return, we re-enter the poll() function which then returns a deserialized version of the message: 
```python
topic = next(topic for topic in self.topics if topic.name() == msg.topic())
item_type_name = dict(msg.headers())['item_type_name'].decode('utf-8')
try:
	with BytesIO(msg.value()) as buff:
		return topic.deserialize(buff, item_type_name)
```
The first line of code is responsible for finding the topic to which this message belongs to.
msg.value() returns the value sent in the record in bytes, which will then be decoded into a string format.
The last line of code is responsible for deserializing the received string using the `deserialize` method.

```python
def deserialize(self, bytes, item_type_name): 
	item_type = type_from_full_name(item_type_name)
	if item_type:
		return item_type(**self.__serializers[item_type_name].deserialize(stream))
	raise TopicSerializationException('Unknown item_type', 'item_type: {}'.format(item_type_name))

```