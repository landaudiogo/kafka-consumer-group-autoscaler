# Kafka Ecosystem


## Kafka
- Brokers
- Topics
- Partitions
- ISR
- Writing and Reading Data from Kafka


### Realiability
#### Production Reliability
- How many in-sync replicas require to have acked.
#### Consumption Reliability
- guarantees the data is read at least **once**
- data quality


## Kafka Connect
- Data centric pipeline: Uses meaningful data abstraction to pull or push data from/into Kafka

### Kafka Connect Source
- e.g. Get data from a data source to insert into a Kafka Topic
### Kafka Connect Sink
- e.g. Fetch the data from Kafka topic to insert into a database
#### Limitations 
- Requires a schema registry


## Schema Registry
### Service
- 100% independant service from Kafka.
- It is simply a service that is polled by a consumer/producer to understand how they might want to deserialize/serialize the data.
- This does not interfere with the process of inserting information into a topic, as the broker does not consult the registry in any time. (if the data is not serialized with the expected schema, it is still inserted into the broker)

### Modularity by references (custom data types)
- Allows a schema to be defined using other types previously defined by the team. (This is also how the modern avro schemas are being created now, it does not function otherwise).
- In other words, we can only define a type once when registering a schema, and from then onwards, we have to reference the created type. (Common issue with types defined in the delivery schema).

### Compatability
- Can be different for different types of schemas.
- Transitive and non-Transitive compatability
	- non-Transitive: A newer version of the schema only has to be compatible with the previous version of the schema.
	- Transitive: All versions in the schema registry have to be compatible with the newest version of the schema.
- Backward Compatability: Consumers using the new schema can read data produced with previous schemas.
- Forward Compatability:  Data produced with a new schema can be read by consumers using a previous schema, even though they may not be able to use the full capabilities of the new schema.
- Full Compatability: Schemas are both backward **and** forward compatible.

### Example Request
Schema to be added to the Schema Registry:
```json
{
    "name": "SMSPhysicalStockUpdates_values", 
    "namespace": "com.huub.types", 
    "type": "record", 
    "fields": [
		{'name': 'timestamp', 'type': 'long'},
		{'name': 'process_reference', 'type': ['null', 'string']},
		{'name': 'variant_id', 'type': 'int'},
		{'name': 'warehouse_id', 'type': 'int'},
		{'name': 'warehouse_name', 'type': 'string'},
		{'name': 'huubclient_id', 'type': 'int'},
		{'name': 'ean', 'type': 'string'},
		{'name': 'reference', 'type': 'string'},
		{'name': 'sales_channel_type', 'type': 'string'},
		{'name': 'delta', 'type': 'int'},
		{'name': 'stock_after_adjust', 'type': 'int'},
	]
}
```

Example request made via postman to the schema-registry service:
```json
POST `/subjects/sms_physical_stock_updated_topic-value/versions`
{
	"schema": "{\"name\": \"SMSPhysicalStockUpdates_values\", \"namespace\": \"com.huub.types\", \"type\": \"record\", \"fields\": [{\"name\": \"timestamp\", \"type\": \"long\"}, {\"name\": \"process_reference\", \"type\": [\"null\", \"string\"]}, {\"name\": \"variant_id\", \"type\": \"int\"}, {\"name\": \"warehouse_id\", \"type\": \"int\"}, {\"name\": \"warehouse_name\", \"type\": \"string\"}, {\"name\": \"huubclient_id\", \"type\": \"int\"}, {\"name\": \"ean\", \"type\": \"string\"}, {\"name\": \"reference\", \"type\": \"string\"}, {\"name\": \"sales_channel_type\", \"type\": \"string\"}, {\"name\": \"delta\", \"type\": \"int\"}, {\"name\": \"stock_after_adjust\", \"type\": \"int\"}]}"
}
```

### Scalabilty / Reliability
- Kubernetes allows for a scalable solution providing with various pods of the same service ready to deliver the data to any request

## Ksql
- Very simple User interface that allows a user to interact very easily with the whole Kafka Ecosystem the company has running.
- Uses sql notation, for quick queries to gather the required information or creating resources.

# Event Consumption

## Custom Consumer
### Current Consumer
1. Consume a single message from a topic;
2. Using the header, determine the avro schema that has to deserialize the message;
3. Deserialize message;
4. Send signals to the interested functions;
5. Commit the message offset.

#### Issues
1. Does not allow for batch consumption;
	- This could be done using the consume() method instead of the poll().
1. The avro schemas currently used with the running version of fastavro, is no longer compatible with the most recent version of the same library (We are stuck with the same version of fastavro);
2. No single source of truth for the avro schemas;
3. When updating a schema's version, for the information to be compatible with all running instances, either all teams have to update their local repositories and redeploy their instances, or there has to be as many topics as versions.
	- This leads to producers having to produce custom data to all the topics of different versions (instead of 1) increasing producer processing time, especially if we are looking for reliability of data production, requiring at least one of the partition replicas to acknowledge the data ingestion.


### DeserializingConsumer
- Very similar to the previous consumer, except for the fact that it already handles deserializing the data;
- Has to be compatible with the most current parsing avro conventions.

Cons: 
- Does not have the consume() method previously mentioned implemented, making it harder for batch consumption vai python.

#### Example Code
Example of using the DeserializingConsumer and the schema registry to consumer records:
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer

# obtaining the schema
schema_registry_conf = {
    'url': 'http://localhost:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
sms_schema = schema_registry_client.get_latest_version('sms_physical_stock_updated_topic-value')
sms_deserializer = AvroDeserializer(schema_registry_client, sms_schema.schema.schema_str)

# consuming and deserializing a message
# https://docs.confluent.io/5.5.1/clients/confluent-kafka-python/_modules/confluent_kafka/deserializing_consumer.html
conf = {
    'bootstrap.servers': '52.213.38.208:9092', 
    'group.id': 'landau_test_consumer0', 
    'value.deserializer': sms_deserializer, 
    'auto.offset.reset': 'earliest'
}
consumer = DeserializingConsumer(conf)
consumer.subscribe(['sms_physical_stock_updated_topic'])
msg = consumer.poll()
```

### Why aren't these solutions optimal for our purposes?
"Consumers created using a librd-kafka based client (Python included, basically anything which isn't Java), use a background thread for the message handling. 

The drawback is that when the consumer process fails the background thread continues sending hearbeats even if the processor dies, and the consumer retains its partitions and the read lag will build until the process is shutdown."

This also means that we depend on a hidden layer in each machine to consume the messages from our topic of interest, with which our python process has to communicate with, to exchange the messages in the thread's memory.



## Kafka Connect Sink
- Simple service accessible via REST API, providing with simple ways to connect
- As our team analyzed, Kafka Connect, allows for a pluggable solution to consume data from our kafka topics, to be inserted into a table in google bigquery.

### Requirements
Schema registry service a Connector can fetch the schema from.

### Pros
- Native Java application, developed by teams with the same requirements as ours, taking into consideration most of the edge cases we will need to consider, and maybe more.
- It's still a Kafka Consumer, and leverages the Consumer groups allowing for distributed data consumption.
- Gets us closer to near real-time data consumption.
- Can be used for data transformation as well, possibly allowing us to get ahead of the ETL process, requiring fewer steps than the ones currently in place.


### Limitations
- As any group of consumers consuming from a single topic, the amount of running instances is limited to the amount of partitions, and therefore, requires some thought before defining the amount of partitions a given topic has. (it's not a static solution as we can always increase the amount of partitions a topic has at any given time)
- Dependant of a schema-registry service.


# Kubernetes and Scalability
## Kubernetes as a Service
- Kubernetes runs as a distributed cluster, providing us with an optimal solution for horizontal scalability.
- Most of the time, we do not require more computing power, as most of the processing time within a service, has to do with network availability.

## Internal Scalability
- By default, Kubernetes allows for an instance to scale based on metrics like CPU and memory, allowing for a service to be available regardless of the load. With exception to the number of pods having reached the limit of instances, or the machines within the Kubernetes cluster, no longer having capacity to run any more instances.

## Custom and External Metrics
Since Kubernetes version 1.6. it provided a new autoscaling API, allowing for other metrics to be taken into consideration when scaling objects. 

Multiple open-source projects have been devoloped to autoscale services being run in kubernetes, heavily dependant in metrics from other sources, other than the kubernetes ecosystem. 

A good example of this is KEDA (Kubernetes Event Driven Autoscaling) which autoscales each deployment based on metrics of the depending service.
e.g. Kafka KEDA: https://keda.sh/docs/1.4/scalers/apache-kafka/.



## Granular Metrics for Scalability (Kubernetes API)
https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#deployment-v1-apps

Making use of Kubernetes API, we can communicate with the Kubernetes for most of the required operations, reinforcing the most prominent requests, such as:
- creating a deployment;
- creating pods;
- deleting pods;


## Running Kubernetes
Currently within AWS, there are 2 ways of running kubernetes. Either bare metal, or EKS.

Both are exactly the same when working with low loads, the difference being when either service is overloaded. 

### Bare Metal

With this approach the cost of having Kubernetes running comes with the cloud instances we choose to have in our cluster. 

When deploying a service kubernetes will choose where each pod will run, trying to distribute the load between all the machines in our cluster. 

This comes with a setback, being that if there is an unexpected load onto our services, no more pods can be created for a given deployment, limiting our horizontal scalability.

### EKS

Service provided by Amazon, which gives us access to the same functionalities we would have within any Kubernetes cluster. The major difference being that whenever we require to increase the amount of pods for our deployments, we don't run out of computing power to do so, as Amazon gives us access to all their machines. 

The cost of running this solution is of 0.10euros/hour for each kubernetes cluster we choose to have. 

As for the remaining costs:
> "You only pay for what you use, as you use it; there are no minimum fees and no upfront commitments." https://aws.amazon.com/eks/pricing/

As is reinforced in the previous link, we can use a single eks cluster to run all our Kubernetes deployments.

# Proposal
This has been a build up to the following solution, which we found is adequate to solve the problems that were presented to our team. 

As context, these are the requirements for our solution:
- Reliability
- Near Real-time
- Monitoring

The remainder of the presentation will be to present with an architecture that will respond to each of these requirements.

## Architecture
[[image]]

## Dependencies
- Kubernetes, or any service that provides distributed scaling.

## Metrics

## Types of Consumers
Any of the consumers previously mentioned: Connect; DeserializingConsumer; our CurrentConsumer, can be used within our solution. But, to achieve a near real-time data consumption, the way a topic is partitioned, will influence how many consumer we can scale up to, limiting our scalability to the amount of partitions we have.

The faster a consumer consumes, the less partitions are required to achieve the same rate of consumption, proving it would be ideal to be using Connect consumers for this purpose.