2 step implementation:
1. Determine how to read the last committed message on the topic
2. Parse the message into a list of consumers based on the existing consumers + unassigned partitions.


# Reading last message

The Kafka Consumer client provides with the seek method which allows the consumer to seek a message with a provided offset. 

1. get the latest offset.
2. use seek to read the message.

# Parse Message

To parse the message, it would be ideal if there were a mapping between partition and the consumer it is currently assigned. This could be done if the controller kept track of a dictionary where the key is the partition, and the value is the consumer object that holds it.

For each of the partitions within the message, use the map to determine which consumer has been assigned that partition. If there is None, add it to the unassigned partitions. Else update the consumer's partition speed.

When updating the consumer speed, verify if it exceeds the maximum consumer capacity. If it does, set the flag `CAPACITY_EXCEEDED`.

Having updated all the partitions, if one of the transitions is set:
- `CAPACITY_EXCEEDED`
- `S1_TIME > MAX_TIME_S1`
- Unassigned partitons is not empty

The execution of the algorithm is triggered.
