# Consumer Procedure

The Consumer goes through 3 important phases within it's process, to approximate it's consumption rate to a constant value when being challenged to work at it's peak performance. These phases repeat cyclically until there the consumer is terminated by an external termination signal, or if an unexpected exception is raised. 

## Phase 1: Gathering Records

The first phase is where data is gathered before being sent to the datawarehouse. The goal is to gather at least as many bytes as BATCH\_BYTES.   

If the amount of time since the beginning of a new cycle exceeds the value stored in the constant WAIT\_TIME\_SECS, then the data is inserted regardless of the amount of bytes gathered from Kafka.

## Phase 2: Processing

The second phase is where the data gather is processed. This is where the data is prepared to be sent into bigquery. Each record fetched from a Kafka topic, represents a new row in a bigquery table. 

Since the gathered records originate from multiple topics, each row has to be batched with data of it's kind, which are rows that are intended to be sent to the same bigquery table.

A single instance of a Batch, a datastructure created to agglomerate rows intended for the same bigquery table, holds all the rows which will be sent in a single post request through Google's python bigquery client API. Because the client limits a single request to ... bytes, each batch is defined with a maximum size of ... bytes.

A BatchList, is another datastructure created to group batches that are to be sent to the same table. If there were no limit in the post request sent through the bigquery client, there would be no need for this structure. 

For every row within the row list, it is then appended to it's respective BatchList, defined by the table the row is to be inserted into. In turn, the BatchList controls which Batch instance the row goes into based on the current capacity of the last instance appended to the BatchList. The procedure is simple, where the row is inserted into the last Batch if it does not exceed the batch's maximum capacity. If it does, another Batch is appended to the BatchList where the row is then inserted. As can be seen, this procedure follows the Next fit algorithm mentioned in \ref{}.

## Phase 3: Sending the Data into Bigquery

This is the final stage of the consumer's insert cycle. There is a list of BatchList instances, each having to be sent to a single bigquery table. To optimize the insert cycle, this phase is done asynchronously between each batch that is to be sent to bigquery.

The pseudocode for this procedure is as follows:
```
coroutines = []
for batchlist in list_batchlist: 
	table = batchlist.table
	for batch in batchlist: 
		coroutines.append(
			bigquery_client.insert_rows(batch, table)
		)
await coroutines		
```

First the coroutines are created, and in the last line, the coroutines are awaited that all are completed. 

When terminated, the insert cycle is completed, and the messages that have been inserted into bigquery are committed.

## Maximum Capacity

The way the problem is formulated assumes the consumer is capable, when required, to achieve a maximum data consumption rate, still to be defined, analogous to the capacity of a bin in a BPP.

With the goal of attaining a value for this maximum bin capacity, the consumer was tested in 3 different scenarios, each requiring the consumer to be working at peak performance.

Peak performance is defined as the case when the sum of the bytes still to be consumed from all the partitions the consumer is assigned to, is bigger than BATCH_BYTES.

The 3 testing scenarios aim to test the consumer's throughput while varying the following: 
\begin{enumerate}
	\item number of tables it has to insert data into;
	\item average bytes available in each of the assigned partitions.
\end{enumerate}

The consequence of having more tables where data has to be directed, increases the amount of requests that have to be made asynchronously which directly affects the third phase. 

As for reducing the average amount of bytes available in the partitions assigned, but still being able to make the consumer work at full capacity, aims to test the first phase of the algorithm, where the data has to be consumed from the partitions assigned from Kafka.

This consumer is based on the Kafka client provided by the confluent_kafka package. When the client begins, it starts a low level consumer implemented in C that runs in the background. As the low-level consumer runs, it buffers messages into a queue until the high level python consumer requests for what it has consumed with either poll() or the consume() methods. The difference between these two methods is that the first returns a single message whereas the second returns a batch of messages defined by num_messages. 

To improve throughput, when a consumer requests for data from the broker, the broker attempts to batch data together before sending it back to the consumer. fetch.max.wait.ms and fetch.max.bytes define how a single request is handled by the broker, wherein the first determines the maximum amount of bytes returned by the broker, and the second, the amount of time the broker can wait before returning the data if fetch.max.bytes is not satisfied.

Reducing the average amount of bytes in each of the partitions assigned, leads to the consumer having to perform more requests to fetch the same amount of data defined by BATCH_BYTES. 

Another important definition is that the consumer is said to have reached it's steady state, hwne the consumer is capable of fetching BATCH_BYTES prior to WAIT_TIME_SECS being triggered. This reflects that the low-level consumer is capable of gathering the necessary amount of bytes into it's buffe, while the high-level consumer is running it's second and third phase of the insert cycle.

For the following test cases, BATCH_BYTES was set to 5,000,000 bytes, and WAIT_TIME_SECS to 1s.

The metrics that was deemed important for the analysis of the consumer's behaviour, were the following:
\begin{itemize}
	\item time to gather BATCH_BYTES
	\item time to process the data 
	\item time to send the data into bigquery
	\item total cycle time
	\item measured throughput
\end{itemize}

These values inherently reflects the time it takes for a consumer to go through each of it's phases, and the rate at which it can consume BATCH_BYTES of data.

The data for these results can be found in the ANNEX in the following section \ref{}, but for the sake of brevity, the metrics will be presented as their average and standard deviations for all iterations.

### Test 1

TABLE TESTING CONDITIONS

Average amount of bytes: 20,251,814
Number of partitions: 32
Number of tables: 1

This test aims to test the best case scenario the consumer can find itself in. All the data it is consuming is directed to the same table, and every partition it visits has enough bytes to satisfy the max.fetch.bytes condition, optimizing the throughput between the low-level consumer and the brokers, as the data is batched together. Since there is only one table to send the data to, the third phase is also optimized as there will only be a single batchlist.

GRAPH for the trend of the insert rate of the consumer.

TABLE CONSUMER METRICS RESULTS


### Test 2

TABLE TESTING CONDITIONS

Average amount of bytes: 863,282
Number of partitions: 116
Number of tables: 5

What can be verified in the results presented in \ref{}, is that as expected the first iteration takes longer in the first phase as the consumer didn't manage to fetch BATCH_BYTES from the several partitions, before WAIT_TIME_SECS, trigerring the second phase after 1 second of the high-level consumer's cycle. 

The same table shown in \ref{}, indicates the consumer reaches it's steady state after a single iteration of it's insert cycle, where from that moment onwards the amount of bytes inserted per cycle is always greater than BATCH_BYTES.

GRAPH for the trend of the insert rate of the consumer.

TABLE CONSUMER METRIC RESULTS

Althought the conditions in which the consumer finds itself are less favorable, the results are still indicative of a stable insert cycle data consumption rate, with a reduced average data rate per cycle.

### Test 3

TABLE TESTING CONDITIONS

Average amount of bytes: 4,711,028 
Number of partitions: 144
Number of tables: 5

This test case combines both of the previous scenarios, where data coming from a single topic suffices to satisfy the BATCH_BYTES condition, but there are iterations that require the consumer to send data to 5 different tables as well as the the data originates from multiple topics. 

For this reason, it aims to test a combination of the first and third phase of the high-level consumer's insert cycle.

GRAPH FOR THE TREND OF THE INSERT RATE OF THE CONSUMER

TABLE CONSUMER METRIC RESULTS

As shown by \ref{}, the results resemble the first testing scenario, differing by approximately 1000 bytes/s on the average measured data rate per cycle.

### Bin Capacity

For the remaining part of this thesis, with the configuration set for BATCH_BYTES = 5,000,000 and WAIT_TIME_SECS = 1, a pessimistic approximation is used to define the bin capacity of a single consumer, setting it at a rate of 2,000,000 bytes/s. 
