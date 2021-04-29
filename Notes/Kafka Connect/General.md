# Kafka Connect

A framework to allow streaming data into and out of kafka.

Kafka Connect is another service that can either source information into kafka or Sink that data from Kafka.

Kafka Connect and schema-registry go hand-in-hand. Using a sink as an example, the schema registry is what allows Connect to interpret the data that's inserted in Kafka, to then pass it on to another data management system.

Applied to the Google Bigquery sink, the connector makes use of the schema registry to convert the key and value pair from hte kafka record.

## Service

Kafka Connect is run on any device as a service and, for full functionality, distributed mode is preferred as it allows for multiple workers to be connected to a single connect cluster. Making use of Kafka's consumer groups, the # of sink workers are limited to the amount of partitions a given topic has. The `group.id` present on the configuration file, is what defines the cluster a worker is connected to.

When starting Kafka Connect, a configuration file has to be passed to the binary, so it can be used by a kafka connect worker. 

After starting a Kafka Connect worker, a rest api is provided by default on port 8083 fo the local machine, with which we can communicate to create more connectors.


## Useful links
> What is Kafka Connect?
> https://docs.confluent.io/platform/current/connect/concepts.html
>
> Detailed description of Kafka Connect concepts:
> https://docs.confluent.io/platform/current/connect/devguide.html
>
> Devolping a Kafka Connector: 
> https://www.confluent.io/blog/create-dynamic-kafka-connect-source-connectors/?_ga=2.58103978.1478212796.1614853990-751672135.1614077014&_gac=1.221084266.1614700727.Cj0KCQiA4feBBhC9ARIsABp_nbUm0-46ajDAZUYyvf_FIFARA3E_Olkk3RAk4nhYztjvy671zVgYPjgaAlhEEALw_wcB
