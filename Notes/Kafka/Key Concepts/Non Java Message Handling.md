
# librd-kafka Based Clients

Consumers created using a librd-kafka based client (Python included, basically anything which isn't Java), use a background thread for the message handling. 

The drawback is that when the consumer process fails the background thread continues sending hearbeats even if the processor dies, and the consumer retains its partitions and the read lag will build until the process is shutdown.