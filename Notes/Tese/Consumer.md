# Determine the capacity of a consumer

## Metrics to keep track of

1. Rate at which the consumer consumes from kafka a constant rate of bytes
2. Number of rows the consumer has to send


### Tests to Perform

1. Consuming from 2 different versions of delivery
2. Consuming from topics that have a little amount of data in them
3. Consuming from a combination of the previous 2 scenarios

## Steps 

1. Consume messages from Kafka
2. Turn the messages into rows
3. Verify if the rows have reached the predefined batch size
4. Create a list of batches with the amount of rows that exceeds by the least amount the lower limit. Leave the other rows in the list of rows.
5. insert the batches into bigquery
6. Repeat the process from 1 to 5.

