# Overview

Keeps the necessary data for a single topic a consumer is assigned to. 

## Attributes

`topic`: HUUB topic class used to deserialize the message
`table`: table to which any message from this topic has to be inserted into
`IGNORE_EVENTS`: messages of this type are to be ignored by the consumer, so basically they should not be consumed.
`partitions`: a set DETopicPartition holding the partitions the consumer is currently assigned to.

## Methods

`deserialize`: calls the HUUB Topic that has to deserialize the message from the topic.

