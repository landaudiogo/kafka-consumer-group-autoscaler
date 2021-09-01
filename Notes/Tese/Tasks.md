# General

- create data-engineering-monitor topic
- create the data-engineering-controller topic
- create the data-engineering-service topic
- determine the data model for the data-engineering-controller topic for each of the events and commands Stop, Start, ...
- read from the data-engineering-service to update each deployment.

# Monitor

- retention.ms set to -1.
- Create the process that keeps track of the bytes for at most the last 30 seconds.
- Produce a message to the topic with the relevant information to be given to the orchestrator. (TopicPartition speed for each partition of interest)

ADDITIONAL
- Create a filter (low-pass) to regulate the rate of change of the measured speed by the monitor.


# Orchestrator

- token credentials to control the kubernetes cluster.
- collect the data from `data-engineering-monitor` topic.
- determine the algorithm that will run in this process. 
- implement the algorithm
- implement the partition reassignment process (managing state).
- determine what the frequency of this algorithm should be.

Additional
- Allow for an update via the data-engineering-service topic.

# Consumer

PHASE 1
- develop a consumer that is capable of consuming from several topics making use of only partition data.
- measure the speed of the consumer while varying the following parameters: number of partitions of the same broker, number of partitions on different topics.

PHASE 2
- downward API to assign the consumer with a partition from the data-engineering-controller topic.
- adapt the consumer to consume from `data-engineering-controller` to allow for partitions to be reassigned.
- implement the partition rebalance in the perspective of the consumer which has to unassign current and assign to new partitions.
- Safe shutdown implementation reading the SIGTERM signal in the process.
- assign a persistent volume to a pod with a PVC, and read the volume on start up to determine the consumer state whether it was a controlled or not shutdown.
- single read and write claim to the persistent volume




