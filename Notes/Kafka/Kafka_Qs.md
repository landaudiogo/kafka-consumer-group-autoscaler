How does Kafka consumer read a message from the log?

How is the Kafka consumer notified that there are messages to be read in the
log?

What happens if the group coordinator crashes Kafka?

What is the group leader?
  - receives a list of active consumers and is responsible for assigning a
      subset of partitions to each consumer.
  - The previous process repeats itself everytime there is a rebalance

To see: 
1. configuration to set the heartbeat frequency and session timeouts


Keywords: 
  Topics
    Parititions

  Consumer Group
    Group coordinator
    Group leader
    Consumer


Concepts: 
  - Rebalancing


# Kafka

KEYWORDS: 
#Cluster, #Broker, #Topics, #Partitions, #partitionLeader, #In-syncReplicas, #Producers, #ConsumerGroup, #GroupCoordinator, #GroupLeader, #Consumer, #Event/Message, #Key, #Value

KEY CONCEPTS: 
#Rebalance, #ConsumerOffset, #

Distributed log running in multiple brokers (servers).
When reading data from the log, the data is directly copied from the disk buffer to the network buffer.

The rest of this document will be used to give an overview as to how Kafka functions. 

### Brokers

Kafka is run as a **Cluster** comprised of one or more servers, each of which are called brokers. 




### Topics

A topic will contain many partitions, which may run on different brokers. 

### Partition

A Topic is subdivided into various partitions. The order is guaranteed within a single partition. When a message is produced with a given key to a topic, it will always end up in the same partition. 

Various parititions in a topic allow various consumers to exist within a consumer group. It's pointless to have more consumer's than partitions as that would make the remaining consumer's go idle. 


### Producers

Only has to connect to a single broker of a topic, which will allow it to publish the messages it desires to the partitions of the topic, regardless of the partition being in the original connecting broker, or on another broker of that same topic.


> https://docs.confluent.io/5.5.2/clients/consumer.html

### Consumer Groups

A consumer group subscribes to a topic, and must have the following elements: **Group Coordinator**, **Group Leader** and **Consumer**(s).

Group coordinator: 
One of the available brokers is set as the group coordinator, and it manages not only the members belonging to a consumer group, as well as the partition assignments. 

The group's ID is hashed to one of the partitions in the **\_\_commit\_offset** topic, and the broker to which this partition belongs is selected as the group coordinator.

When the consumer start's up, it finds the coordinator for the group, and send a request to join. The coordinator triggers a rebalance is now triggered so that the new partition receives it's fair share of partitions to be consumed.

Each member of the group, must send offsets to the coordinator in order to remain a member of the group. If no heartbeat is received before expiration of the configured session timeout, then the coordinator kicks that member and assigns it's partitions to other members (rebalance).


### Offset management

When a group is first created, before consuming any messages, each consumer which has been assigned a partition, must determine the starting position for each of it's partition.

As a consumer reads the messages from a partition, it must commit the offsets corresponding to the messages it has read. If a consumer shuts down it's partitions will be assigned to another group member, which will begin consumption from the last commited offset of each partition. If the consumer crashes before any offset has been commited then the new consumer will use the reset policy.

By default the consumer is set to auto-commit offsets. Auto-commit works as a cron with a preiod set via the `auto.commit.interval.ms` configuration property. If a consumer crashes, then after a restart or rebalance, the partitions owned by the crashed consumer will be reset to the last commited offset. When this happens the last auto-commit can be as old as the auto-commit interval. Any messages received since then will have to be read again.

To reduce the amount of duplicates, the auto-commit interval has to be reduced. The consumer therefore supports a commit API, which allows for full control over offsets. when using the commit API, first the auto-commit has to be disabled by setting `enable.auto.commit = false`.

Each call to the commit API is an offset commit request to the broker. 
Using the synchronous commit API, the consumer is blocked until the request returns successfully. 

To reduce throuhput loss when manually commiting, the consumer has a setting `fetch.min.bytes` which returns how much data is returned in each fetch. The broker will also hold on to the fetch until there is enough data is available (or until `fetch.max.wait.ms` expires). The trade-off is that more duplicates have to be handled in a worst-case failure.

There is also an option to use asynchronous commits. Instead of waiting for the request to complete, the consumer can send the request, and return immediately. 

The problem with asynchronous requests is in the case of a request failing, th next batch might have already been processed. Check the callback API which can be leveraged in the case. Ordering always has to be handled.

`enable.auto.commit`: (bool) whether autommit is enabled
`auto.commit.interval.ms`: time between autocommit messages from the consumer side
`auto.offset.reset`: defines the behavior of the consumer when there is no committed position (which would be the case when the group is first initialized), or when an offset is out of the range. either set the offset to the `"earlist"` or the `"latest"` offset (also the default option). `"none"` is also an option if the offset is to be manually determined. 


### Rebalance

Each rebalance has 2 phases: partition revocation and partition assignment.
Partition revocation is always called before partition reassignment, and is the last chance to commit offsets before the partitions are reassigned.

The assignment method is always called after a rebalance, and can be used to set the initial position of the assigned partitions. In this case, the revocation hook is used to commit the current offsets synchronously.

### Consumer Configuration

The only required property is the bootstrap.servers, but a client.id should be set to more easily relate a request made at the broker level, with the client instance that made it. Typically all consumers within the same group will use the same client.id.

### Group Configuration

Group.id should always be configured.

session timeout can be overriden by setting `session.timeout.ms`. This avoids excessive rebalancing. The drawback of increasing this value is that it takes the coordinator longer to detect a dead consumer, which means it will take even longer for another consumer to take over it's partitions.

`heartbeat.interval.ms` is another configurable setting which determines the frequency with which a consumer sends a heartbeat to the coordinator. It is also the way the consumer detects when rebalancing is needed. Lower heartbeat --> faster rebalancing.

`max.poll.interval.ms`: maximum time allowed between calls to the consumer poll method, before the consumer process is assumed to have failed.


### Message Handling

Consumers created using a librd-kafka based client (Python included, basically anything which isn't Java), use a background thread for the message handling. 

The drawback of this is that the when the consumer process fails the background thread continues in sending heartbeats even if the processor dies, and the consumer retains its partitions and the read lag will build until the process is shutdown.

