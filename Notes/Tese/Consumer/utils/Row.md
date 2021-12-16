# Overview 

A row is an instance of a msg consumed from kafka converted into this data structure, which has all the necessary information as to where it has to be inserted in bigquery, and the msg metadata to then be committed after the row is inserted into bigquery.


## Attributes

`topic` - kafka topic name
`partition` - partition number
`offset` - message offset in the partition
`payload` - the message payload that is in the necessary structure to be inserted into bigquery
`payload_size` - The size of the payload in bytes
`table` - the table name where the payload is to be inserted
