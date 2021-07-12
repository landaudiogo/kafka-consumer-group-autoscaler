# Consumers


Specifies the topic name, group, and a list of brokers to connect to, and Kafka will automatically take care of pulling the data from the right brokers.

Messages are read in order w.r.t. a single partition. 
Messages are read in parallel w.r.t. multiple partitions.

Consumers are organized into consumer groups. 
- Each consumer from within a group reads from exclusive partitions.
In other words, no 2 consumers in the same consumer group read from the same partition.

How do consumers know where to read from?
kafka stores the offsets at which a consumer group has been reading for each partition. 
- This information lies in a Kafka topic named `__consumer_offsets`.
- When a consumer has processed data received from Kafka, it should be commiting the offsets, which then changes the consumer
- In the case a consumer dies, then it will be able to read from where it left of due to this consumer offset.

## Consumer Group and Partition Rebalancing

> Confluent Documentation for a consumer:
> https://docs.confluent.io/5.5.2/clients/consumer.html
>
> Oreilly graphical explanation:
> https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html

A consumer group subscribes to a topic, and must have the following elements: **Group Coordinator**, **Group Leader** and **Consumer**(s).

### Group coordinator 

One of the available brokers is set as the group coordinator, and it manages not only the members belonging to a consumer group, as well as the partition assignments. 

The group's ID is hashed to one of the partitions in the **\_\_commit\_offset** topic, and the broker to which this partition belongs is selected as the group coordinator.

When the consumer start's up, it finds the coordinator for the group, and send a request to join. The coordinator triggers a rebalance so that the new consumer receives it's fair share of partitions to be consumed.

A consumer group shares the topic load (number of partitions) with all it's consumers. For this reason, there can only be as many consumers as there are partitions to achieve maximum parallelism.

Each member of the group, must send heartbeats to the coordinator in order to remain a member of the group. If no heartbeat is received before expiration of the configured session timeout, then the coordinator kicks that member and assigns it's partitions to other members (rebalance).

### Rebalancing

When a consumer is added to a consumer group it reads from partitions previously read by another consumer. The same thing happens when a consumer shuts down or crashes, it leaves the group, and the partitions it used to consume will be consumed by one of the remaining consumers. Reassignement also occurs when the topics are modified, e.g. partitions are added to a topic.

Moving partition ownership from one consumer to another is called rebalancing. Rebalancing is what provides scalability and availability. this allows us to safely add and remove consumers. 

In the normal course of events, rebalancing is a long process as it doesn't allow consumers to consume messages for its duration. When partitions are removed from one consumer to another, the consumer loses its current state (Loses its caches), so any messages that haven't been commited, will be duplicately read.

The way consumers maintain membership in a consumer group and ownership of the partitions assigned to them is by sending hearbeats to a Kafka broker designated as the **GROUP COORDINATOR** (can be different for different consumer groups). The consumer is assumed to be alive, as long as it is sending heartbeats at regular intervals. Heartbeats are sent when the **consumer polls** and when it **commits records** it has consumed.

If the consumer stops sending hearbeats for long enough, its session will time out and the group coordinator will consider it dead and trigger a rebalance. If a consumer crashed and stopped processing messages, it will take the group coordinator a few seconds without hearbeats to decide it is dead and trigger the rebalance. During those seconds, the partition consumed by this consumer is not consumed until the process of rebalancing is terminated.

When closing a consumer cleanly the group coordinator triggers the rebalance immediately, reducing the gap in processing.

**Example**
Suppose you have an application that needs to read messages from a Kafka topic, run some validations against them, and write the results to another data store. In this case your application will create a consumer object, subscirbe to the appropriate topic, and start receiving messages, validating them and writing the results.

If there are many producers writing to the same topic, quite quickly the single consumer will not be able to keep up with the messages being written to the topic.

Here is where the multiple consumers comes in handy, or in other words a **Consumer Group**. When multiple consumers are (subscribed to the same topic) &&  (belong to the same consumer group) --> each consumer in the group will receive messages from a different subset of the paritions in the topic.

### Configuration
`session.timeout.ms`: Overriding this configuration, may avoid excessive rebalancing. The drawback of increasing this timeout, is that it takes longer for a group coordinator to determine when a consumer is dead. (countered by kubernetes)
`heartbeat.interval.ms`:  is another configurable setting which determines the frequency with which a consumer sends a heartbeat to the coordinator. It is also the way the consumer detects when rebalancing is needed. Lower heartbeat --> faster rebalancing.
`max.poll.interval.ms`: maximum time allowed between calls to the consumer poll method, before, the consumer process is assumed to have failed.