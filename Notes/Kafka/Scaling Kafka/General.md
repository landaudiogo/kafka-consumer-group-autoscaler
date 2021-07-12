+

# Scaling Kafka

> Confluent documentation
> https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
>
> How to choose the number of topics/partitions in a kafka cluster:
> https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/?_ga=2.135114164.406015498.1615299582-751672135.1614077014&_gac=1.149903556.1614700727.Cj0KCQiA4feBBhC9ARIsABp_nbUm0-46ajDAZUYyvf_FIFARA3E_Olkk3RAk4nhYztjvy671zVgYPjgaAlhEEALw_wcB
>
> Updated version of the previous article with respect to the controller and broker failover:
> https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/?_ga=2.140428982.406015498.1615299582-751672135.1614077014&_gac=1.48648916.1614700727.Cj0KCQiA4feBBhC9ARIsABp_nbUm0-46ajDAZUYyvf_FIFARA3E_Olkk3RAk4nhYztjvy671zVgYPjgaAlhEEALw_wcB
>
> Benchmarking: 
> https://docs.confluent.io/cloud/current/client-apps/optimizing/index.html#ccloud-benchmarking




## Increasing the partitions per topic

The number of partitions per topic is what limits the amount how parallelized a process can become. To calculate the necessary partitions there has to be a **target throughput** (Tt).
After determining the target throughput, the **consumer throughput** (Ct) and **producer throughput** (Pt) are used to specify the amount of partitions required. 

consumer_partitions = Tt/Ct
producer_partitions = Tt/Pt
number_partitions = max(consumer_partitions, producer_partitions)

When increasing the number of partitions over time, one has to be extra careful, when producing messages with keys. If a message contains a key, it is guaranteed that another message with the same key will end up in the same partition. That condition doesn't necessarily hold when the number of partitions changes over time. Due to this, it is common to overpartition a bit.

### More partitions requires more file handles

Kafka uses a very large number of files and a large number of sockets to communicate with the clients. All of this requires a relatively high number of available file descriptors.

Many modern Linux distributions ship with only 1,024 file descriptors allowed per process. This is too low for Kafka. You should increase your file descriptor count to at least 100,000. This process can be difficult and is highly dependent on your particular OS and distribution.

### More partitions may increase unavailability

When a broker fails, partitions with a leader on that broker become temporarily unavailable. Kafka will automatically move the leader of those unavailable partitions to some other replicas to continue serving the client requests. This process is done by one of the Kafka brokers designated as the controller. It involves reading and writing some metadata for each affected partition in ZooKeeper. Currently, operations to ZooKeeper are done serially in the controller.

In the scenario of a broker being closed cleanly, each of the partitions this broker has being leader, are passed on to replica partitions one at a time, which on the consumer side present just a few ms of unavailability.

However, if the broker is shutdown unexpectedly, the observed unavailability could be proportional to the number of partitions the broker had.

If the failing broker is the Controller, the process of electing new partition leaders won't start until the controller fails over to a new broker. The failing over occurs automatically, but requires the new controller to read some metadata for every partition in the ZooKeeper. if this reading takes 2ms per partition, and there are 10000 partitions, another 20s of unavalability before the new partition leaders can be assigned.

### More partitions may increase end-to-end latency

The end-to-end latency in Kafka is defined by the time from when a message is published by the producer to when the message is read by the consumer. Kafka only exposes a message to a consumer after it has been committed, i.e., when the message is replicated to all the in-sync replicas.

By default, a Kafka broker only uses a single thread to replicate data from another broker, for all partitions that share replicas between the two brokers. (does this mean there is a single thread per broker?)

Experiments done by the article presented above, have shown that having 1000 partitions being replicated between 2 brokers can add at least 20ms of latency.

On a larger cluster, this problem is alleviated considering there is a broker that is a partition leader to 1000 partitions, then the sum of the replicas in the other 9 brokers has to be 1000 partitions. On average the other brokers will only have 1000/9 ~ 111 partitions, and therefore the amount of partitions the thread has to replicate is only 111 compared to the previous 1000.

### More partitions might imply more memory on the producer 

When producing messages there is an option where the messages are batched on the consumer side per partition. Only when the upper limit is reached for each batch on the producer is it sent to the broker. If there are many partitions, then this means that the producer may have to store more messages on its side before it sends the data.

To avoid this, the producer must be configured with a larger memory size.

## Compression

Compressing can be used to send less bits to represent more bits. This is done by setting the `compression.type` parameter.

