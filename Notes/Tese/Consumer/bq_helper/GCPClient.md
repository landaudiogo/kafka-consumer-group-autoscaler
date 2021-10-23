# Overview

Class that is supposed to help with the interactions between the process and the bigquery client.

## Attributes 

`credentials`: credential file to access a bigquery project
`bq_session`: Google bigquery python client 
`storage_session`: Google storage python client

## Methods

`send_bq`: Receive a list of batchlists as input, and send each batchlist to their respective tables.
`send_table`: Receive a single batchlist and go through the necessary procedure to send the data into the table.
`stream_batch`: Send a single Batch, which corresponds to a single request, into bigquery. When sending a batch of rows into bigquery, it is possible that a set of rows fails. 
`use_bucket`: Send rows into bigquery using a bucket.
`__enter__`: Enter procedure for the context manager.
`__exit__`: Closing procedure when exiting the context manager.