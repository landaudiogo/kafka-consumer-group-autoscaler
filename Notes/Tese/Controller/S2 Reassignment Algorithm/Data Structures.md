# TopicPartition:

**Attributes**
`topic`
`partition`
`speed`

# Topic
**Attributes**
`partitions`: set of partitions the consumer is currently consuming from of a given topic.

# DictTopic
Each element is a key value pair, where the key is the topic string, and the value is the Topic datastructure. 

**Methods**
`combined_partitions`: union of all partition sets in each topic.

# Consumer
**Attributes**
`dictTopic`: instance of DictTopic that contains the topics this consumer is fetching data from

