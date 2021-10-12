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

Each iteration of the monitor process goes through the following steps: 
1. Measure the sie of the partitions
2. push into the queue
3. remove all the elements at the back of the queue that have been measured more than 30 seconds ago
4. measure the speed using the last element and the first element of the queue
5. Produce message to monitor topic with the most recent speed measurement


# Container

To maintain coherence between systems, we shall define environment variables that allow the system to understand it's context.

As such, the following variables are defined: 
- Environment: (uat, prod)
- brokers: (broker:29092)

The state of the container is stored in a persistent volume, which will be analyzed on startup. This will contain information as to which topics are to be analyzed.

# Tasks

# Testing

1. Everytime the producer send a message to the partition, it indicates the speed at which it is writing the data, this is the amount of bytes it sent, over the time difference since the last time it sent a message. 
2. The monitor process computes the current speed of writing the message, and writes to a temporary topic only the write speed of the same partition of the previous example.
3. Create a Graph that compares the speed values for the consumer and for the monitor process.

## Steps 

1. Add Timeline class to the producer so the time difference between the last produced message and the current can be computed. 


## Test 1

This test aims to show how long it takes the monitor process to stabilize on the current partition write speed.

The producer sets a time difference between sending the message to the partition, which will last for at least 35 seconds.

## Test 2

Randomize the time difference between the last message sent by the producer between 0.01 and 2 seconds.
