# Consumer Config

2. max.partition.fetch.bytes - the maximum amount of data per-partition the
   server will return. Records are fetched by the consumer in batches. 
4. fetch.wait.max.ms - The maximum amount of time the server will block before
   answering the fetch request if there isn't sufficient data to immediately
   satisfy the requirement given by fetch.min.bytes.
5. message.max.bytes - Maximum Kafka protocol request message size. 
6. receive.message.max.bytes - Maximum Kafka protocol response message size. 
7. max.in.flight - Maximum number of in-flight requests per broker connection. 
8. socket.timeout.ms - Default timeout for network requests. Consumer will use
   fetch.wait.max.ms + socket.timeout.ms.
9. queued.min.messages - Minimum number of messages per topic+partition
   librdkafak tries to maintain in the local consumer queue.
10. queued.max.messages.kbytes - Maximum number of kbytes of queued pre-fetched
    messages in the local consumer queue. if using the high-level consumer this
    setting applies to the single consumer queue, regardless of the number of
    partitions. This value may be overshot by fetch.messgae.max.bytes.
11. fetch.wait.max.ms - Maximum time the broker may wait to fill the Fetch
    Responsewith fetch.min.bytes of messages.
12. fetch.message.max.bytes - Initial maximum number fo bytes per
    topic+partition to rqeuest when fetching messages from the broker.
13. fetch.max.bytes - Maximum amount of data the broker shall return for a fetch
    request. 
14. fetch.min.bytes - The minimum amount of data for the server to return unless
   the request times out
