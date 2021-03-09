
# Producer


To publish data, producers only have to specify the topic name, and one of the brokers of the kafka cluster, and Kafka automatically takes care of routing the data to the correct brokers and partitions. 

When a producer sends data it will be randomly assigned to a partition of that topic (if no key is provided)

Producers have differente options as to how much the data they are writing to the topic can be acknowledged:
- Acks: 0 --> Will not wait for an acknowledgement and continously send the data.
- Acks: 1 --> Wait for acknowledgement from the partition leader (limited data loss)
- Acks: all --> Waits for acknowledgement from leader and replicas (no data loss)

If a key is sent with a message, then it is guaranteed that all the messages with that key will end up in the same partition.

After writing data into a partition it cannot be removed (immutability).

## Configuration

`linger.ms`: How long the producer waits before sending messages to the partition. 
`batch.size:`: with this configuration set, everytime the producer wants to send a message to a topic, it first batches the messages it wants to send by partition, and when it has reached this size, it send to the partition.

