# Overview

Wrapper of the confluent_kafka consumer which gives the consumer more functionality, closer to HUUBs implementation of a consumer. 

## Attributes

`current_assignment`: map between the kafka topic name and a DETopic instance that represents the current partitions the client is consuming from this same topic.
`change_state_queue`: when an internal message is received to change the consumer's state, it is enqueued onto this variable which is then used to be processed at once to change the consumer's assignment.
`row_list`: a RowList instance which holds all the rows that have already been consumed by the consumer, but are waiting to be sent to bigquery.

## Methods

`process_state_queue`: 