# Overview

The objective of the work elaborated @ HUUB, is to develop a dynamic service which is capable of consuming messages from a kafka topic and inserting them into big query tables.

Kafka supports scalability, but does not provide with the tools to automatically scale a service. Therefore, the work is to propose an architecture which allows this dynamic and automatic scalability based on current producer throughput.

The data-engineering team is interested on a group of topics, and each event on a single topic has to be inserted in its respective big-query table. 

The data-engineering-monitor topic is where the metrics produced by the monitor go to, to be consumed by the orchestrator to then evaluate the current system setup based on these metrics.

The data-engineering-controller is where the messages related to the partition consumption are sent to.
This topic can be viewed as a "control" topic, where each partition is associated to a single consumer, and each consumer reads messages related to the partitions they are supposed to consume from, and events such as start consumption and stopped consumption are sent to partition 0 which is read solely by the orchestrator/controller.

The data-engineering-service where configurations of this service are sent to. This means that if there is another topic to be consumed from, or another version of the same topic, this topic is what will inform the controller what has to be done.

# Monitor

Scala process that continuously queries the logdirs from the kafka brokers. This provides with the amount of bytes in each partition the broker owns. In two different time instances, if there are messages being produced, to a given partition, the total amount of bytes will differ. As such, this process measures the difference in bytes, and determines the write speed to each partition of interest.

For the purpose of this work, the write speed will be determined in a 30 second window, and the total difference of bytes within that window will determine the write speed in bytes/second. The process can also query mulitple times for the data, and append a timestamp to the query, allowing it to keep in memory the bytes to be considered for a single throughput measure, and providing with a solution of how the measure frequency can be higher than if it were in a 30 second period.

The TopicPartition throughput can then be a record that is inserted into a monitoring topic, to be read by the controller. (data-engineering-monitor)

**Optional**:
Additional filters may be taken into consideration (low-pass), to reduce the frequency with which this metric oscillates.

# Controller (Orchestrator)

This is the brain of the whole architecture. It gathers the data produced by the monitor, and computes the amount of consumers required, and which partitions are assigned to which consumers.

There are 2 possible approaches to this problem, each with their advantages.

## Algorithm 1

The problem is the one of bin packing, and there are several articles on the topic that provide multiple heuristics, and even operational research algorithms.

Periodically the orchestrator runs the algorithm which determines the number of consumers which are required to fit the amount of items with a specified weight (production rate) in containers with a certain capacity (consumer throughput)

A mixed linear programming problem is proposed.

Data: 
- partition's write speed: This is the information produced by the monitor process. Each partition that belongs to the topics of interest are taken into consideration.
- single consumer speed: This is the speed at which a consumer can function. Tests have to be performed to determine the average speed the generic-consumer achieves in bytes/second.
- Speed margin per partitions: This metric is for a single consumer, and the speed margin is multiplied by the number of partitions assigned to it. The more partitions it is assigned, the more margin it requires.

Objective Function: 
The objective function is to try and minimize the number of consumers required, as it is directly associated with the costs of the deployment.

Constraints: 
- A single partition can only be assigned to a single consumer.
- The sum of the partition's speed assigned to a single partition, plus it's speed margin associated to the number of partitions associated to it, has to be less than the single consumer speed.

### OR approach

> Google OR team:
> https://developers.google.com/optimization/bin/bin_packin

Based on Mixed Integer Programming, this module presents a result of the consumers (bins) each partition (item) is assigned to.

### Best fit Decreasing (BFD)

Order the partitions in a decreasing manner. 

For each item, determine the bin where it fits the tightest. If it doesn't fit, create a new bin.

To optimize this algorithm, use a binary search tree for the bins.


#### Adaptation of the BFD

For each consumer there is an ordered list for the partitions it is consuming from based on the partition speed in decreasing order.

For each of these lists, the following steps are followed.
1. Attempt to fit the smallest elements in consumer's other than the ones they are assigned to, and pop them off the list.
2. If a smaller element does not fit, then the consumer it is currently assigned to is considered a valid bin, and the partitions are inserted in a decreasing order now. 
3. If a partition does not fit in the current consumer bin, then the list is not processed anymore, and we move on to the next list and repeat from the first step.
4. When this is performed for all the lists, the remaining partitions are assigned to the consumers in a BFD fashion.


## Algorithm 2 (Not what will be implemented, but should be mentioned as future work)

Non-linear Programming Problem.

Objective Funciton:
Minimize the Variance of the sum of partition's write speed in each consumer.

## Managing and Informing Consumers

As soon as the algorithm runs, the output provided is the one of the partitions that are to be assigned to the consumers. 

in case the algorithm determines that more (or less) consumers are required, the orchestrator then updates the deployments and it either creates or deletes specific deployments. But to do so, it first has to guarantee that the consumer stops consuming from it's current partitions, and only then can it exit safely.

The orchestrator has to inform each consumer what partitions it has to consume from. Given that the orchestrator has information as to what partitions are currently being consumed, it has to determine the partitions that no longer should be consumed from a consumer, and send a StopConsumeCommand to each consumer with the relevant information. As soon as the orchestrator reads the StopConsumeEvent, it can then send the StartConsumeCommand to the respective consumers. Posterior to receiving the StartConsumeEvent, it enters idle mode, until this process has to be performed again.

As a summary the orchestrator has 3 states:
- Idle: Here the orchestrator might be idle because it is forced to wait after a rebalance, or it is waiting for a consumer to be overloaded based on the partitions it is currently assigned. These are the 2 ways it triggers the computation of which partitions are to be asssigned to each consumer (deployment).
- Managing: In this state, the orchestrator is responsible for guaranteeing that a consumer is capable of subscribing to the correct partitions, and that there are no overlapping subscriptions to the same partitions.

## Frequency

The frequency at which this algorithm should run is uncertain, but there are several possibilities, each with their downsides:

### Continuous process

Can react to certain demands a lot faster, but has the problem of possibly providing different results each time, triggering the rebalancing process too often, slowing the consumer throughput. 

### Periodic:

Would be a similar approach to how the Kubernetes rebalancing algorithm is performed, as in it runs every 30s. 

In case a rescale is triggered, if the output is the one of upscaling, 3 minutes have to have passed since the last rescale, whereas in the case of downscale, 5 minutes have to pass since the last rescale.

# Consumer
Due to the specificity of this algorithm, nor the stateful set nor a single deployment satisfy the conditions this algorithm requires to reduce the impact of the rebalancing process.

This happens because within a deployment when a single pod is "deleted" it is randomly chosen from the group, and because the orchestrator/controller wants the control as to which consumers are deleted, this cannot happen. 

Another counter to using a single deployment would be the fact that we could not assign a single volume to each consumer to preserve the state of a given consumer (which partitions it is assigned to).

The stateful set solves the previous problem but the first remains as when a SS is downscaled it does so from the last created consumer to the first. As previously mentioned, this is not necessarily the case when running the previous algorithms so it would only worsen the rebalance affects.

The alternative is to use multiple deployments each with a single consumer with a volume assigned to it. 

Because neither a statefulset nor a deployment allow for specific pods to be deleted, a different approach has to be used to guarantee data persistence and an improved dynamic partition rebalance. As such, the suggested approach is to use a group of deployments that contain a single pod, that always access the same volume. The volume would be a single pod read and write, not allowing for 2 pods from the same deployment to be spawned at the same time. This means that there may be some downtime, but it avoids 2 pods reading from the same partitions simultaneously. Making use of the deployment name, the pod can determine from which partition it has to consume from. data-engineering-$id (1,2,...,N).

To be compatible with rolling updates, there has to be a guarantee that no more than a single pod exists in a single deployment. 
The following link describes the options that have to go to the yaml file of the deployment.
> Stack overflow question
> https://stackoverflow.com/questions/52848176/re-attach-volume-claim-on-deployment-update
> Documentation:
> https://kubernetes.io/docs/tasks/run-application/run-single-instance-stateful-application/#updating

The consumer will also require a persistent volume claim. As such, the orchestrator is responsible for creating the persistent volume and the Persistent Volume Claim (PVC). When the deployment is started, the PVC is then added in the yaml file.

To access metadata in the container/pod, the downward API is used to allow the pod to determine from which partition it will consume from in the data-engineering-controller topic. The deployment name is what will define the partition to be consumed from.
> https://stackoverflow.com/questions/42274229/kubernetes-deployment-name-from-within-a-pod

When a deployment is deleted, it will send the SIGTERM signal to the pods in the deployment. As such, the process running in the consumer should listen to this signal, as to safely exit the process, and send all the required messages from within the container. It is important to have the process running as PID1 to receive the signal in the first place.
> https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-terminating-with-grace
> description of why the exec for must be used in the Dockerfile:
> https://tasdikrahman.me/2019/04/24/handling-singals-for-applications-in-kubernetes-docker/
> Stack overflow on how to handle graceful killing on python:
> https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully

The control topic will have as many partitions as the maximum amount of consumers the group has already had. Using the ordinal id, a single consumer will assign to itself the same partition of the control topic. 

All consumers in the group have the same group id but never consume from the same partitions in the control-topic. This allows for a consumer that has it's ordinal id, to keep consuming from where it left off.

The events that are inserted into the control topic are of the following nature: 
- StartConsumeCommand: This message is dispatched by the controller to indicate the consumer subscribed to the partition which TopicPartitions it must assign to itself, and to which BQ table the messages are to be inserted.
- StartConsumeEvent: Produced by the Consumer when it has assigned the TopicPartitions mentioned in the command specified above.
- StopConsumingCommand: Dispatched by the controller to indicate a consumer the partitions it must stop consuming from
- StopConsumingEvent: Produced by the connector when it stops consuming from the TopicPartitions mentioned in the previous command.

After a message is consumed from it's partition in the control-topic, the consumer raises a flag to determine that there has to be a partition reassignment. It finishes sending the messages to the bigquery tables, and then reassigns the partitions.

This reassignment has to be done making use of the following methods: 
-  `Consumer.incremental_unassign()`: This method accepts as parameter a list of `TopicPartition`, and unassigns the consumer from the specified partitions. This is the method that is called when a StopConsumeCommand is received.
- `Consumer.assign()`: This method accepts as parameter a list of `TopicPartition`, and assigns the consumer to the specified partitions. This is the method to be run, when a StartConsumeEvent is received.

The controller is also subscribed to this control topic, to keep the consumer's states up to date on it's side.

When a consumer updates it's state, it writes to it's own assigned volume it's current state. This is to allow a consumer to fail, and when dispatched again, to continue consuming from where it left off without having to consume all the records on it's partition to reconstruct it's state. And this is why there is a need for a Persistent volume for each consumer.


