# Overview

Holds a mapping between topics and DETopicPartitions and the event type (StartConsumeCommand, StopConsumeComand) to indicate whether those elements are to be removed or added to the current assignment.

## Attributes

`event_type`: string holding the type of the msg which is incorporated in the header. (StartConsumeCommand, StopConsumeCommand).
`topic_map`: map with the key representing the kafka topic name, and the value being a DETopicPartition.