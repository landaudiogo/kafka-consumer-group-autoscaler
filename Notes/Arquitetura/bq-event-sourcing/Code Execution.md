# Code Organization

## Useful Links
> Official Confluent Kafka documentation for the python module: 
> https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer






## Execution

This repository leverages the django module to run the kafka consumer code.

### Django start up

Teh root directory contains a Dockerfile which describes the container environment.

At the end of this file there is a CMD call, which on container startup will run the following command: 
```bash
python manage.py kafka_consumer --env prod
```

manage.py is the file that django uses to determine the list of commands it has available to it. In this case it will be looking for a command called `kafka_consumer`.

Because this command does not belong to the native options django has avaiable, this command has to be set in the `delivery_to_bigquery/management/commands` directory. 

There, the python module `kafka_consumer.py` contains a `Command` class which also contains the `handle()` function. This function is called when the aforementioned CMD is executed at the end of the Dockerfile.

### Creating the Consumer

When creating an instance of the `Command`, there are a few attributes set in the constructor that help the Kafka Consumer understand it's context.

the following attributes are set on instance creation `__init__`: 
- `topics`: 
```python
self.topics = [ settings.CONSUMER_TOPICS['DELIVERY_EVENTS_V4_TOPIC'] ]
# This would be equivalent to doing 
self.topics = [ DeliveryEventsV6Topic() ]
```
When the DeliveryEventsV6Topic is initialized, so is it's subtopics (e.g. DeliveryOnHoldV6Serializer), and within these last, each initialize it's own AvroSerializer, with schema and item_type when calling the constructor.
```python
class DeliveryOnHoldV6Serializer(AvroSerializer):
	def __init__(self, item_type=DeliveryOnHoldV6):
		schema = {
			'name': 'DeliveryOnHoldV6',
			'type': 'record',
			'fields': DeliveryEventV6AvroSchema
		}
		super().__init__(schema, item_type)
```
The last line of code, initializes the AvroSchema, which in turn parses the schema, into the AvroSchema instance attribute, `__parsed_schema`. This is posteriorly used by the deserialize method to call the schemaless reader.
```python
class AvroSerializer(Serializer):

    def __init__(self, schema, item_type=dict):
        super().__init__(item_type)
        self.__parsed_schema = parse_schema(schema)
		
	def deserialize(self, stream):
		try:
			return schemaless_reader(stream, self.__parsed_schema)
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
- `poll_timeout`: ? 

When the kafka_consumer command is run, the `handle` function is called.

It begins with a context_manager, which creates a `ConfluentKafkaConsumer` with the following properties set on the `__init__()` method. 

```python
# The None argument provides the possibility of adding a logger
with ConfluentKafkaConsumer(self.config, self.topics, None) as consumer:
```
Not only does the above code create an instance of the called class, it also runs the `__enter__` method defined in the class. This last method is used to create a native consumer from the **confluent_kafka** module. 

from the `__enter__` method: 
```python
self.consumer = Consumer(self.conf)
self.consumer.subscribe([topic.name() for topic in self.topics], on_assign=self.__consumer_assignment_report)
```
The first line of the previous code, creates the Consumer with the configuration dictionary given in the Command's `__init__` method, whereas the second defines the topics the consumer subscribes to. In the scope of the bigquery_event_sourcing repository, it only subscribes to the `"delivery_events_v6_topic"` topic in kafka.

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
msg.header() returns the headers the message contains. the header containing the value for the following key `"item_type_name"` is fetched to then be passed to the topic's deserialize method. (This determines the "type" of record that was sent to this topic so it can be deserialized).
msg.value() returns the value sent in the record in bytes, which is then deserialized by the topics deserialize method. The key item_type_name is what determines which deserialize method is going to be used.

```python
def deserialize(self, bytes, item_type_name): 
	item_type = type_from_full_name(item_type_name)
	if item_type:
		return item_type(**self.__serializers[item_type_name].deserialize(stream))
	raise TopicSerializationException('Unknown item_type', 'item_type: {}'.format(item_type_name))

```
On this excerpt of code, `item_type` is found using the item_type_name provided by the header of the Message received in poll.

The method `type_from_full_name` then uses the string to separate the parts using the '.' character to then perform an absolute import of the module, followed by the import of the item_type in the same module using the last part of the string. 

The line making reference to `self.__serializers[item_type_name]`, uses the full name provided in the header via the item_type_name key, to determine which serializer class that was used to serialize the record. It then calls the deserialize method from the `AvroSerializer` class:
```python
def deserialize(self, stream):
	try:
		try:
			return schemaless_reader(stream, self.__parsed_schema)
		except UnicodeDecodeError:
			stream.seek(0)
			return next(reader(stream, self.__parsed_schema))
	except Exception as e:
		raise SerializationException() from e
```

and this terminates the stack of calls to poll a message.

### Sending the signal 

In the scope of the bq-event-sourcing, the only signal that is going to be triggered is the `signals.delivery_changed`.

Due to the call to 
```python
from django.conf import settings
```
the list specified in the settings.py: 
```python
INSTALLED_APPS = [
	..., 
	delivery_to_bigquery.apps.DeliveryBigQueryConfig,
]
```
allows django to initialize the apps.

within the DeliveryBigQueryConfig app: 
```python
class DeliveryBigqueryConfig(AppConfig):
    name = 'delivery_to_bigquery'

    def ready(self):
        from delivery_to_bigquery.delivery_changed_handler import handler as delivery_changed_handler
        from delivery_to_bigquery.shipping_costs_handler import handler as carrier_account_handler
        from delivery_to_bigquery.ims_handler import handler as ims_handler
        from delivery_to_bigquery.shipping_prices_handler import handler as shipping_prices_handler
        from delivery_to_bigquery.physical_stock_handler import handler as physical_stock_handler
        from delivery_to_bigquery.signals import delivery_changed, shipping_costs, ims_updates, shipping_prices, physical_stock

        delivery_changed.connect(delivery_changed_handler, dispatch_uid='delivery_changed_handler')
        shipping_costs.connect(carrier_account_handler, dispatch_uid='carrier_account_handler')
        ims_updates.connect(ims_handler, dispatch_uid='ims_handler')
        shipping_prices.connect(shipping_prices_handler, dispatch_uid = 'shipping_prices')
        physical_stock.connect(physical_stock_handler, dispatch_uid = 'consumer_sms_physical_stock')

```
the signals we wish to connect to are initialized.

Now that we already have the handlers connected, we can understand what happens after consuming a single message from kafka.

After consuming the message the following line is the one that triggers the event:
```python
signals.delivery_changed.send(sender=self.__class__, evt=msg, env=options['env'])
```

and as we saw in the `ready()` method, the method that will be called with the following event is the `delivery_changed_handler`.

the arguments passed in the `send` method are: 
- evt: the deserialized kafka message
- env: the environment the application is running in (this is passed in the startup command called at the end of the Dockerfile).

### delivery_changed Event handler

The first step is to create a list of rows to be inserted in the database.
In the current use case, there is only a single row being inserted therefore it is a list of a single element.

When creating the row element, the evt attributes are called to get the elements for the bigquery table.
The required attributes are: 
```python
'timestamp'=evt.timestamp, 
'event_id'=evt.id, 
'event_type'=evt.__class__.__name__, 
'event_json'=json.dumps(evt.__as_dict()), 
'topic_version'=6,
'is_current_version'=True, 
'stream_timestamp'=time.time_ns(), 
'stream_timestamp_date'=time.strftime("%Y-%m-%d", time.gmtime()),
'stream_timestamp_hour=time.strftime("%Y-%m-%d %H:00:00", time.gmtime())'
```