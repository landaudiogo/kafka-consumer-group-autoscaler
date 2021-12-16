# Overview

This is where the controller sends records to each consumer to change their consumption metadata to attempt to change their state into the desired configuration. 

# Requirements

This state depends on: 
- Controller's current assignment: This is the current state the consumers find themselves in. 
- Controller's future assignment: This is the state the controller wants to lead the consumers into. 

# Procedure

1. Initially a difference is computed between the controller's future state and the controller's current state. This provides with information which regards the messages that have to be sent to the consumer's (this includes stop and start commands). There might be partitions that the consumers have not yet started consuming from in the current assignment, these are also prepared to be sent.
2. The controller sends all the stop consuming commands to the respective consumers, and the StartConsumingCommands that were prepared in the condiitons mentioned in the previous step.
3. The controller now enters a state where it is waiting for each consumer that has received the stop command to send a StopConsumingEvent, and for StartConsumingEvent messages that are sent after each consumer acknowledges having started to consume from the partitions within the StartConsumingCommand. For each message it receives in it's partition in the data-engineering-controller topic it receives, depending on whether it is a StartConsumingEvent or a StopConsumingEvent, the controller does the following: 
	1. If the message received is the StopConsuming Event, the controller now prepares the StartConsumingCommand messages to be sent to the consumers that are to start consuming from the partitions mentioned in the StopConsumingEvent.
	2. If the message received is a StartConsumingEvent, then the partition is removed from some type of tracker, that indicates which partitions are not in their final consumption state by a consumer. Send the StartConsumingCommand messages to the consumers.


# Development

```python
def __sub__(self, other):
	for c 


```

## ConsumerListDiff 

Which consumers to remove
which consumer to create
StopConsumingCommands
Which consumer's have to receive a StartConsumeCommand.


