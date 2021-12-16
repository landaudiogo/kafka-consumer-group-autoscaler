# Overview

This process is intended to track the consumer's load and guarantee that a single consumer is not overworked.

This process runs indefinitely, and it has several states where it finds itself in.

## Sentinel state

The first, is a state where it analyzes the speed metrics, and has the following triggers to go into the second state:
- the partitions assigned to one or more consumer's have their combined speeds higher than the consumer's maximum capacity
- more than 30s (e.g. time interval which is still to be defined)

## Computing Reassignement

This state is responsible for running the bin packing algorithm to determine which partitions are assigned to which consumers.

The input for the algorithm is the partition speed and a mapping from the partitions that are currently assigned to a consumer and unassigned. 

The output is the new consumer mapping the controller has to send to each consumer through the controller topic.

As soon as the algorithm has finished, the transition is triggered to move to the third state (changing consumer state)

## Changing Consumer State

Required to inform the consumers of the new partitions they have to fetch data from, and the ones they have to stop consuming from.

Firstly the orchestrator part of this process is responsible for creating the consumers that still do not exist in the cluster.

When assigning a partition to another consumer, the controller has to know which consumer to send the stopconsumingcommand, and only after receiving acknowledgement from this consumer does it send the startconsumingcommand to the new consumer.

The first step is to send the stop command messages to all the consumer's that no longer continue processing the partitions within the message.

The controller then moves on to a state where it waits to receive all the acknowledgements through the StopConsumingEvent. For each of these messages, a StartConsumingCommand is prepared, and then sent if the consumer is already up in the cluster.

After sending the StartConsumingCommand, the controller also expects to receive a StartConsumingEvent. When this happens, the controller can remove the partitions within the record from it's wait_queue.

if the wait_queue is empty, it triggers the transition back to the first state.

# Additional considerations

Capable of handling unassigned partitions.
Capable of subscribing to a new topic.
Updating topic metadata for the consumers.
