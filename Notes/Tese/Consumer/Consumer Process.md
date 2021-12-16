# Overview 

In a simplified manner, the consumer is responsible for fetching the data from the partitions it is assigned from, and insert the data from each topic into their respective bigquery table. 


# Process

1. The controller assigns the consumer it's partitions from the metadata data-engineering-controller topic.
2. The consumer then starts iterating on getting the data from kafka and placing it in the bigquery tables.

## Iteration

1. The consumer has a buffer of messages it has read from the kafka client. If the amount of bytes gathered is bigger than the `batch_bytes variable` or if the amount of time elapsed since the beginning of the consumption cycle is bigger than `wait_time`, then the rows gathered are inserted into bigquery (if there are any).

	1.1. If the condition is verified, then the consumer goes through three phases	
	
		1.1.1. `process`
		1.1.2. `send_bq`

	1.2. If the codition cannot be verified (else) then the consumer gets more rows from the Kafka Client, and goes back to step 1.1.
	
		1.2.1. consumer.consume()

This is described with the following pseudo-code:
```python
while 1: 
	if (row_list.bytes > batch_bytes) or (time_elapsed > wait_time):
		list_batchlist = pre_process(row_list)
		send_bq(list_batchlist)
		consumer.commit()
	else: 
		row_list.extend(consumer.consume(num_messages=1_000_000, timeout=0))
```


## Reliability

The process should be reliable in the following scenarios: 
1. A row cannot be deserialized
2. When streaming a batch, the result comes back with errors in certain rows.
3. When streaming a batch, the whole request fails.
4. When sending a batch of rows through a bucket, it cannot be inserted into bigquery.


What to do when each of the above scenarios occurs:
1. heehhe
2. Same as 1
3. Split the original batch into 2, and try again. (Current implementation sends all rows individually).
4. We can either leave the file in the bucket, or we can use a similar proccess as 1.