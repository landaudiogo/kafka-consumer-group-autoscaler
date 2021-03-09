# Topic & Partitions

A topic will contain many partitions, which may run on different brokers. 

A Topic is subdivided into various partitions. The order is guaranteed within a single partition. When a message is produced with a given key to a topic, it will always end up in the same partition. 

Various parititions in a topic allow various consumers to exist within a consumer group. It's pointless to have more consumer's than partitions as that would make the remaining consumer's go idle. 

When creating a topic, the following arguments have to be provided: `replication-factor`, `topic` and `partitions`

When creating a topic with multiple partitions, they will be spread around the brokers, and depending on the replication factor, for a single partition, it's replicas will be on another broker, which provides for redundancy for the case the partition leader fails unexpectedly. 

A partition replica, tries to be in-sync with the partition leader, and when it does, it becomes an In-Sync Replica (ISR).

### Partitions
Determines the amount of partitions the topic will have spread in the different brokers. 

This is Kafka'a unit of parallelism. The more parititions there are, the more parallel produceres and consumers there can be, writing/reading to/from partitions.

### Topic replication factor
- this sets the amount of replicas of a paritition that will exist. So if the TRF is 2, then there will be 2 partitons which are the same wrt their data in 2 different brokers. 
- There can only be one partition leader, and this is the partition that reads and writes data. The remaining partitions in other brokers, exist to synchronize their data, which are replicas. 
- When the replica has synced the latest message from the partition leader, then it is an in-sync replica (ISR).

