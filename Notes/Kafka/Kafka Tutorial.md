# Kafka as a service

## Requirements

### Confluent Platform

#### Debian & Ubuntu
> https://docs.confluent.io/4.0.3/installation/installing_cp.html#debian-and-ubuntu

#### Docker
> https://docs.confluent.io/4.0.3/installation/docker/docs/quickstart.html

```bash
git clone https://github.com/landaudiogo/kafka-tutorial
cd kafka
docker-compose up -d
cd ..
```

This starts the services required to have a fully functional Kafka Service.

A simple description of what each service provides:
1. Broker: Runs a single broker in a cluster, allowing topic creation. Since there is only a single broker, replication-factor is not allowed as there is no other broker to hold the partition replica. Multiple partitions is still allowed though, but they are all stored in a single machine.
2. Zookeeper: handles the brokers associated with it.
3. Schema-registry: Holds the schemas that describe each topics format. The default subject naming strategy is  TopicNameStrategy, which has the following structure: \<topic_name\>-\<record-key\>. 
	As an example, suppose there is a topic in our broker named "test". When a message is to be decoded by kafka connect, if the record-key is the value to be decoded, then the node searches within the schema-registry for a subject named "test-key". A similar approach is taken when the value is to be decoded, except for the fact that the subject name to be searched for would be "test-value". 
	For custom consumers, the naming strategy is subjective to the developer, but a good standard would be to adopt the TopicNameStrategy. Also provides a REST API to communicate with, allowing for simple subject and schema creation on port 8081.
4. Connect: Kafka Connect node that accepts outside modifications using the port on 8083.
5. ksql: another confluent service that allows for simple interactions with the kafka services. Due to the configurations within the docker-compose file, ksql communicates with the different services to execute a given sql command to manipulate resources.

Now that we have our kafka cluster running, we are going to start our personal services that will run a postgres database and a pgadmin service allowing us to store the data consumed by a Kafka Connect node onto a table. 

```bash
cd huub_db
docker-compose up -d
cd ..
```

By default docker-compose does not connect services from different docker-compose cluster's into the same network. Meaning that if our "connect" service wants to communicate with our "postgres_huub" service, there must be some kind of network routing defined in our docker-compose configuration. That is why we added a network field in each service, allowing these services to communicate with one another. 

Now that we have this set up, we can start messing with our kafka service to provide us with some intuition as to how these pieces all fit in together.

These next steps will allow us to have a Kafka Connect Worker that streams data from a Kafka Topic into a table in postgres. For this a driver has to be installed:

```bash
# Entering our connect container
docker exec -it connect bash

# installing a jdbc driver
confluent-hub install confluentinc/kafka-connect-jdbc:10.1.0

exit
```

To allow record production and consumption a topic has to exist within our Kafka cluster (in our case, a single broker). Therefore, the following command is run, to create a topic named sms_physical_stock_updated_topic:

```bash
# creating a kafka topic
docker exec broker \
  kafka-topics --zookeeper zookeeper:2181 \
  --create \
  --topic sms_physical_stock_updated_topic \
  --partitions 1
```

Now that we have our topic created, we would like to get full functionality from our Kafka services, which takes us to creating a single schema subject within our schema-registry. We will name this subject "sms_physical_stock_updated_topic-value". As previously mentioned, this is the naming strategy that our Kafka connect node will be using to find the schema it requires for a given record.

Here we can either open up postman, insomnia or any other application that allows to create simple http requests, or we can use curl within our terminals.

For simplicity, I will be choosing the curl service as you can simply copy and paste this command: 

```bash
cd schema-registry

# making the request to the schema-registry service to create a subject
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @./sms_schema \ 
  http://localhost:8081
  
cd ..
```

Now that we have our schema described in our schema registry, we can interpret the records that are located in the broker, but for this, we need to have records there in the first place. 

We will be using the messages that have been inserted into HUUB's production brokers into the topic "sms_physical_stock_updated_topic". 

Because confluent's avro deserializer is not compatible with fastavro's serialized schema, we first have to replicate the data in the production broker into our broker. 

For this we run the following commands: 

```bash
cd data-replica

docker-compose up 

cd ..
```

What the previous container does for us, is create a consumer that will deserialize the data for us using fastavro's deserializer, to then produce the data into our custom topic, but serialized using confluent's avro serializer. The schema's are exactly the same to us, but the serializing method is different.

This will allow our Kafka Connect node to interpret the data that has been inserted into the topic. 

Now that we already have our data in our topic, we can proceed with creating a connect node, to consume the data and to insert it into a table, in our database.

```bash
cd connect-node

curl -X POST \
  -H "Content-Type: application/json" \
  --data @./sms_connect_node \ 
  http://localhost:8083
  
cd ..
```

Hopefully, if everything worked as expected, we now have a database in our local system, that contains all the messages that have been inserted into huub's production broker in the topic sms_physical_stock_updated_topic, ats a row in our table.


