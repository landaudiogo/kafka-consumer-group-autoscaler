# To Do

## Guarantee message consumption is reliable

Where can a message be consumed but not populated?
- test whether the consumer is capable of incremental assign in the expected way.
- when python consumes the message from the kafka client
- when processing the row_list
- when sending the message in a batch to bigquery
- if bigquery batch fails then send each row one by one
- if sending single row fails

## Determine action sequence for controller message

## row_list should be a part of the consumer