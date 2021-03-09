https://www.confluent.io/blog/apache-kafka-for-service-architectures/


When a service wants to read messages from Kafka it seeks the position of the last message it read, then scans sequentially, reading messages in order, while periodically recording its new position in the log.

Writes to the log are append only.

When reading the messages from Kafka, the data is copied directly from the disk buffer to the network buffer. 

Kafka is many logs, a collection of files, filled with messages, spannign many different machines. Kafka's code involves tying the different logs together, routing messages from producers to consumers reliably, replicating for fault tolerance and handling failure.

Kafka is itself a cluster.

Producers spread messages over many partitions, on many machines, where each parition is a little queue. 

**What are kafka consumer groups?** 

Load balanced consumers (denoted a Conseumer Group) share hte partitions between them.

Topics with hundreds of terabytes is not uncommon, and with bandwidths carved up between the various services, using the multi-tenancy freatures makes clusters easy to share.

### Consumer and Consumer Group

> https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html

Suppose you have an application that needs to read messages from a Kafka topic, run some calidations against them, and write the results to another data store. In this case yuor application will create a consumer object, subscirbe to the appropriate topic, and start receiving messages, validating htem and writing the results. 

If there are many producers writing to the same topic, quite quickly the single consumer will not be able to keep up with the messages being written to the topic.

Here enters the concept of multiple consumers, or a **Consumer Group**. This allows multiple consumers to read the data from the log, and spllitting the same data between them.

When multiple consumers are (subscribed to the same topic) &&  (belong to the same consumer group) --> each consumer in the group will receive messages from a different subset of the paritions in the topic.

Example: 

1. Topic **T1** with four partitions. 
2. Consumer **C1**, only consumer in group **G1**, subscribed to **T1**.

In this scenario **C1** will receive all messages from all 4 partitions.

3. Add another Consumer **C2** to group **G1**.

Now each consumer will receive messages coming from 2 different partitions. 

4. **G1** has 4 consumers. 

In this scenario the 4 consumers receive messages from different a single paritions (1 of the 4 partitions of T1).

if there are more consumers in a Consumer group than partitions, some of the consumers will be idle and get no messages at all.

The main way to scale data consumption is by adding more consumers to a consumer group. 

This is a good reason to create a topic with a large number of paritions, this allows to add more consumers when the load increases. **There is no piont in adding more consumers thannyou have partitions in a topic.**

How do you choose the number of partitions? (Chapter 2 of this article)

In addition to adding consumers (to a consumer group) in order to scale a single application, it is very common to have multiple applications that need to read data from the same topic.

In order to have an application receive all messages from a topic then we need to make sure that the application has it's own consumer group that is subscribed to the topic.

In summary:
1. A new consumer group for each application that has to read from one or more topics. 
2. Consumers are added to a consumer group to scale the reading and processing of messages from the topics. 


### Consumer Groups and partition Rebalance

When a consumer is added to a consumer group it reads from partitions previously read by another consumer. The same thing happens when a consumer shuts down or crashes, it leaves the group, and the partitions it used to consume will be consumed bu one of the remaining consumers. Reassignement also occurs when the topics are modified, e.g. partitions are added to a topic.

Moving partition ownership from one consumer to another is called rebalancing. Rebalancing is what provides scalability and availability. this allows us to safely add and remove consumers. 

In the normal course of events, rebalancing is undesirable as it doesn't allow consumers to consume messages, providing with unavaiilability of the entire consumer group. When partitions are removed from one consumer to another, the consumer loses its current state (Loses its caches).

The way consumers maintain membership in a consumer group and ownership of the partitions assigned to them is by sending hearbeats to a Kafka broker designated as the **GROUP COORDINATOR** (can be different for different consumer groups). The consumer is assumed to be alive while as long as it is sending heartbeats at regular intervals. Heartbeats are sent when the **consumer polls** (i.e. retrieves records) and when it **commits records it has consumed**.

If the consumer stops sending hearbeats for long enough, its session will time out and the group coorinator will consider it dead and trigger a rebalance. If a consumer crashed and stopped processing messages, it will take the group coorinator a few seconds without hearbeats to deecide it is dead and trigger the rebalance. During those seconds, the partition consumed by this consumer are no longer consumed until the rebalance occurs.

When closing a consumer cleanly the group coordinator triggers the rebalance immediately, reducing the gap in processing. 

### How does assigning partitions to brokers work?

When a consumer wants to join a group, it sends a JoinGroup request to the group coodinator. The first consumer to join the group becomes the group leader. 

The leader receives a list of all consumers in the group from the group coodinator (this will include all consumers that sent a heartbeat recently and which are therefore considered alive) and is responsible for assigning a subsset of partitions to each consumer. This process repeats itself everytime a rebalance is triggered by the group coordinator.

### Creating a consumer

The properties required when creating a consumer are: 
- bootstrap.servers (string): connection to a Kafka cluster. 
- key.deserializer: 
- value.deserializer:

### Brokers

A kafka cluster is composed of multiple brokers (servers)
Each broker has it's own ID (integer)
After connecting to a broker, you will be connected to an entire cluster.

Topic replication factor
- this sets the amount of replicas of a paritition that will exist. So if the TRF is 2, then there will be 2 paritions which are the same wrt their data in 2 different brokers. 
- There can only be one partition leader, and this is the partition that reads and writes data. The remaining partitions in other brokers, exist to synchronize their data, in-sync replica (ISR).


### Producers

To publish data, producers only have to specify the topic name, and one of the brokers, and Kafka automatically takes care of routing the data to the correct brokers and partitions. 

When a producer sends data it will be randomly assigned to a partition of that topic (if no key is provided)

Producers have to choose how much they want their data being written has to be acknowledged:
- Acks: 0 --> Will not wait for an acknowledgement and continously send the data.
- Acks: 1 --> Wait for acknowledgement from the leader (limited data loss)
- Acks: all --> Waits for acknowledgement from leader and replicas (no data loss)

If a key is sent with a message, then it is guaranteed that all the messages with that key will end up in the same partition.

### Consumers

Read data from the topic

Specifies the topic name and one broker to connect to, and Kafka will automatically take care of pulling the data from the right brokers.

Messages are read in order w.r.t. a single partition. 
Messages are read in parallel w.r.t. multiple partitions.

Consumers are organized into consumer groups. 
- Each consumer from within a group reads from exclusive partitions

How do consumers know where to read from?
kafka stores the offsets at which a consumer group has been reading 
- This information lies in a Kafka topic named `__consumer_offsets`.
- When a consumer has processed data received from Kafka, it should be commiting the offsets. 
- In the case a consumer dies, then it will be able to read from where it left of due to this consumer offsets.


After writing data into a partition it cannot be removed (immutability).






