# Loading Data into Bigquery


### Batch loading

**This is NOT a real time solution**

With this method, it's usually loading data via a file with a given format. It does not satisfy the real-time constraints of the company so we have to look for a streaming alternative.
The best alternative is using a google storage instance which thenn adds the data to bigquery 


### Streaming

> https://hevodata.com/blog/apache-kafka-to-bigquery/#s2 

There are 2 alternatives in this scenario, which scale horizontally: 

##### Creating a custom streaming connector between Kafka and Big Query

Using kubernetes, a given process can consume data from a kafka topic to then populate the Big Query tables. 
Using this alternative there is the possibility of setting up the exact parameters that comply with a given latency objective. There might be a few concerns to be taken into considerations to provide an assurance that the reads are reliable.

> Custom metrics in kubernetes using Keda (uses consumer lag in message reading):
> https://medium.com/faun/event-driven-autoscaling-for-kubernetes-with-kafka-keda-d68490200812

Questions: 

##### Using Kafka Connect Clusters


> What is Kafka Connect?
> https://docs.confluent.io/platform/current/connect/concepts.html
>
> Detailed description of Kafka Connect concepts:
> https://docs.confluent.io/platform/current/connect/devguide.html
>
> Devolping a Kafka Connector: 
> https://www.confluent.io/blog/create-dynamic-kafka-connect-source-connectors/?_ga=2.58103978.1478212796.1614853990-751672135.1614077014&_gac=1.221084266.1614700727.Cj0KCQiA4feBBhC9ARIsABp_nbUm0-46ajDAZUYyvf_FIFARA3E_Olkk3RAk4nhYztjvy671zVgYPjgaAlhEEALw_wcB

Can be run in a distributed mode which allows for a more reliable service. Kafka connect doesn't take care of redeploying the instances, so it is recommended that kubernetes be used either way.

Key concepts: 
workers -> connectors -> tasks -> converters -> transforms

Connector: 
- Does not perform data copying, but via it's configuration, describes the set of data to be copied, and the connector is responsible for breaking the job into a set of tasks that can be assigned to kafka connect workers.
- Can also be responsible for monitoring the data changes of the external systems, and request task reconfiguration.

Tasks: 
- Allows for a distributed running mode, providing scalability, and state is stored not within the tasks, but in kafka in the following topics: `status.storage.topic` and `config.storage.topic`
- Similar to a consumer group, when 2 connectors connect to the same `group.id`, the tasks and connectors associated to the consumer group are rebalanced between all the members of the cluster.
- **Rebalancing** is triggered when: 
	- connector's increase or decrease the amount of tasks they require
	- connector's configuration file is changed
	- When the amount of workers increases or decreases

Workers: 
- Connector's and tasks are logical units of work, and have to be executed in a process. These processes are called **Workers**. 
- Distributed Workers: 
	- In this mode, several workers are started with the same `group.id` and they automatically coordinate to schedule execution of connectors and tasks across all workers.

Monitoring Kafka Connect:
- Kafka Connect, which is intended to be run as a service, contains a REST API, which enables administration of the cluster. 
- The request can be made to any of the cluster member's


##### Kafka Connect Bigquery Sink

The bigquery sink expects a schema description on the topic, which specifies how the table should look like
The schema table allows for a one on one conversion from a topic to a table
What is the paralel limitation of the kafka sink connector (how many parallel tasks)?
- Expected: 1 task per partition

> Example of applying the bigquery sink
> https://www.youtube.com/watch?v=kJfC8sOw504&ab_channel=Kafka%26Bigdata%26Cloud
> 
> Official Repository for BigQuery sink connector:
> https://github.com/confluentinc/kafka-connect-bigquery
>
> Kafka Connect Big Query Sink connector: 
> https://docs.confluent.io/5.0.4/connect/kafka-connect-bigquery/index.html
> 
> Configuration options:
> https://docs.confluent.io/kafka-connect-bigquery/current/kafka_connect_bigquery_config.html#kafka-connect-bigquery-config
> 
> Getting started: 
> https://docs.confluent.io/home/connect/userguide.html#operating-environment
> 
> Schema registry:
> https://docs.confluent.io/platform/current/schema-registry/index.html#schemaregistry-intro
>
> Monitoring Kafka Connect: 
> https://docs.confluent.io/home/connect/monitoring.html

##### Google Dataflow