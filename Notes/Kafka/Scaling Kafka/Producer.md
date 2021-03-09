# Scaling the Producer

## Producer throughput

The per-partition throughput that one can achieve on the producer depends on configurations such as the batching size, compression codec, type of acknowledgement, replication factor, etc.

## Producer Acks

When a producer sends a message to the leader partition in a broker, depending on the configuration, it might have to wait for a response from the leader broker (as long as `acks != 0`) to know that it's message has been committed before sending any other messages.


## Summary Configurations
-   `batch.size`: increase to 100000–200000 (default 16384)
-   `linger.ms`: increase to 10–100 (default 0)
-   `compression.type=lz4` (default `none`, for example, no compression)
-   `acks=1` (default 1)
-   `buffer.memory`: increase if there are a lot of partitions (default 33554432)