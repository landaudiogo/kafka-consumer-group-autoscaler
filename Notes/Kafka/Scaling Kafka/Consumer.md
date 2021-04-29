# Scaling the Consumer

## Links

> Scaling the Consumer using consumer kafka metrics: 
> https://strimzi.io/blog/2021/01/07/consumer-tuning/

## Consumer Throughput

The consumer throughput is often application dependent since it corresponds to how fast the consumer logic can process each message. 

To increase throughput, the `fetch.min.bytes` can be set. Read "Key Concepts/Consumer.md", to understand what this parameter does.



## Summary Configurations

-   `fetch.min.bytes`: increase to ~100000 (default 1)
-   consider batch consuming: https://stackoverflow.com/questions/45920608/how-to-read-batch-messages-in-confluent-kafka-python/45932153

