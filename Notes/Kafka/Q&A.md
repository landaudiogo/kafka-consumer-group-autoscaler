Q: How does Kafka consumer read a message from the log?
A: A consumer is continuosly polling its partitions to gather whether there is any new information, and in case there is, the data is consumed. 
After reading the record, it commits the message, and the kafka the offset is stores in the `__consumer_offsets` topic in Kafka.


Q: What happens if the group coordinator crashes?
A: When a group coordinator crashes (broker crashed) the 