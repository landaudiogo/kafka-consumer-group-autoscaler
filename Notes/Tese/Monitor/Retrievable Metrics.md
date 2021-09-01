# Very Important Assumption

This is only accurate because we assume that retention.ms is set to -1, so the partitions can grow indefinitely. 

# Java

## Kafka Admin Client 

```Java
describeLogDirs(
	Colleciton<Integer> brokers,
	DescribeLogDirsOptions options
)
```



```
public DescribeTopicsResult describeTopics(
	Collection<String> topicNames,
	DescribeTopicsOptions options,
)
```


Args:
topicNames - a set that contains the topics to get the data from.
options - does not really have to be set.

Returns:
**DescribeTopicsResult**

This class contains the following callable methods: 
`all()` - Return `KafkaFuture<Map<String, TopicDescription>>`

`class TopicDescription` contains the following method: 
`partitions()` - which returns `List<TopicPartitionInfo>`

`class TopicPartitionInfo` has an attribute `leader` which is of the type `Node`

`class Node` has as attribute `id` that indicates the broker id that is leader for this partition


## Retrieving the relevant data

`describeLogDirs()` is responsible for getting the batch information for the partition size for each replica in all brokers. 

`describeTopics()` would then be used to filter which broker the information for that partition is relevant.

## Processing the information

A queue is stored for the size of each relevant partition, and it's size. As soon as a metric is processed using the above process, then it would be appended to the queue with a timestamp. After doing so, we would pop any element from the queue, that contains an epoch that has been measured more than 30 seconds ago from the new measurement. As a final step, the difference between the first and last measurement in bytes / difference in timestamps would determine the write rate to the partition.


# Container

To maintain coherence between systems, we shall define environment variables that allow the system to understand it's context.

As such, the following variables are defined: 
- Environment: (uat, prod)
- brokers: (broker:29092)

The state of the container is stored in a persistent volume, which will be analyzed on startup. This will contain information as to which topics are to be analyzed.

# Tasks

