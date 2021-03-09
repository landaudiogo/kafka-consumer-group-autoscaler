# Offset Management

When a group is first created, before consuming any messages, each consumer which has been assigned a partition, must determine the starting position for each of it's partition.

As a consumer reads the messages from a partition, it must commit the offsets corresponding to the messages it has read. If a consumer shuts down it's partitions will be assigned to another group member, which will begin consumption from the last commited offset of each partition. If the consumer crashes before any offset has been commited then the new consumer will use the reset policy.

By default the consumer is set to auto-commit offsets. Auto-commit works as a cron with a preiod set via the `auto.commit.interval.ms` configuration property. If a consumer crashes, then after a restart or rebalance, the partitions owned by the crashed consumer will be reset to the last commited offset. When this happens the last auto-commit can be as old as the auto-commit interval. Any messages received since then will have to be read again.

## Commit API
To reduce the amount of duplicates, the auto-commit interval has to be reduced. The consumer therefore supports a commit API, which allows for full control over offsets. when using the commit API, first the auto-commit has to be disabled by setting `enable.auto.commit = false`.
Each call to the commit API is an offset commit request to the broker. 

### Synchronous Commit

Using the synchronous commit API, the consumer is blocked until the request returns successfully. 

To reduce throuhput loss when manually commiting, the consumer has a setting `fetch.min.bytes` which returns how much data is returned in each fetch. The broker will also hold on to the fetch until enough data is available (or until `fetch.max.wait.ms` expires). The trade-off is that more duplicates have to be handled in a worst-case failure.

### Asynchronous Commit

There is also an option to use asynchronous commits. Instead of waiting for the request to complete, the consumer can send the request, and return immediately. 

The problem with asynchronous requests is in the case of a request failing, the next batch might have already been processed. Check the callback API which can be leveraged in the case. Ordering always has to be handled.

### Configuration

`enable.auto.commit`: (bool) whether autommit is enabled
`auto.commit.interval.ms`: time between autocommit messages from the consumer side
`auto.offset.reset`: defines the behavior of the consumer when there is no committed position (which would be the case when the group is first initialized), or when an offset is out of the range. either set the offset to the `"earliest"` or the `"latest"` offset (also the default option). `"none"` is also an option if the offset is to be manually determined. 
`fetch.min.bytes`: The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. This means that the server will send the response as soon as this amount of bytes are available or the fetch request times out, waiting for data to arrive.
`fetch.max.wait.ms`: The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by `fetch.min.bytes`.