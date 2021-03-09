# Scaling the Consumer

## Consumer Throughput

The consumer throughput is often application dependent since it corresponds to how fast the consumer logic can process each message. 

To increase throughput, the `fetch.min.bytes` can be set. Read "Key Concepts/Consumer.md", to understand what this parameter does.


## Summary Configurations

-   `fetch.min.bytes`: increase to ~100000 (default 1)