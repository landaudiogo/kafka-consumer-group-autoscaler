# Overview

A Class which is intended to pack all the rows that are intended to a single table from bigquery.

## Attributes

`total_bytes` - the amount of bytes that have already been appended to the instance.
`total_rows` - the total amount of rows that have been added to the instance.
`weak_max_batch_size` - weak because this limit can be exceeded if a single message is bigger than this limit.
`to_bucket` - In case a row is too big to be sent using an API, this value is assumed to be 9_000_000 bytes, then the row is added to this list.
`table` - string indicating which bigquery table the rows are supposed to go to.
`tp_offsets` - map which has as key a tuple (topic_name, partition_number), and as value the last offset appended to an instance.
`tp_start_offsets` - Similar to the above attribute, stores the first offset for each topicpartition appended to this instance.

## Methods

`add_row()` - Intended to control how a row is added to an instance to guarantee the control attributes previously mentioned are correctly updated.
`last_bin()` - returns the last open bin in the list of batches.
`tp_list_commit()` - Based on the attribute `tp_offsets` a list of TopicPartition with offsets is provided intended to be sent to in the `confluent_kafka.Consumer.commit()` method.

