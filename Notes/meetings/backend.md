



Duvidas: 


1. Quantos brokers?
2. Quantos zookeepers? (default)
3. Quantas partições por topico? (so existe 1 partição por topico)
4. Quantas replicas? (nao)
5. Quais são os tópicos que já existem? (15 ja implementam varios group consumers)
6. No caso da partição à marca, os dados sao persistidos numa base de dados, correto?
7. Qual a lógica de termos 1 consumidor por topico?
8. Em que maquina esta a definição do kafka e os brokers, com as suas configurações?



Conclusões:
1. Kafka com SSL certificate (usam avro no certificates) 
2. Definir Tópicos
3. Rebalance automático, ter a quantidade de partiçoes para o pior caso, e depois conforme o throughput esta a aumentar, aumentar a quantidade de cosumidores.
4. Temos atrasos a popular na base de dados, depois de consumir o evento
5. Possibilidade de rollback
6. Por projeto tem 1 consumer por determinado topico


### Scalability

For each of these calculations, this indicates the amount of partitions each of these services require, working in parallel to achieve the objective throughput. 

Due to the nature of this architecture, everything being produced has to be consumed by a consumer group. Therefore, the amount of partiitons is strongly defined by the worst case throughput between the producers and the consumers. 




After setting the amount of partitions, kafka supports adding partitions on runtime, but due to its design, it does not support reducing the number of partitions wihtout any sideeffects. E.g. It cannot remove a partition wihtout losing the data it already contains.

On the other hand, when adding a partition, only the new messages will be considered.

### Produce time

Time between when the application produces a record with Producer.send(), and when the produce request containing the record is sent to the leader broker.

A Kafka producer batches records for the same topic partition to optimize network and IO requests issued to kafka.

linger.ms - how long the producer waits for more records to send as a single batch.
batch.size - maximum amount of records the producer can send in a single produce request.
compression.type - if the producer compresses the bath to be sent.

The producer might have to wait longer if the number of unacknowledged produce requests sent to the leader broker has already reached its limit.(by default this value is 5)

### Scaling Consumer

http://www.vinsguru.com/kafka-scaling-consumers-out-for-a-consumer-group/

Runs an example with docker compose which increases the number of consumers with the following command: 
`docker compose --scale consumer=N`, N being the number of consumer containers we want to run.

Uber's implementation with Kafka cluster Mirrorring (Doesn't seem like our solution at the moment)
https://eng.uber.com/ureplicator-apache-kafka-replicator/


Scaling the consumers with kubernetes:
https://blog.softwaremill.com/autoscaling-kafka-streams-applications-with-kubernetes-9aed2e37d3a0

Kubernetes and keda for scaling Kafka: 
https://medium.com/faun/event-driven-autoscaling-for-kubernetes-with-kafka-keda-d68490200812
General autoscaling can be done using Keda and Kubernetes
https://keda.sh/docs/2.1/scalers/apache-kafka/ How KEDA scales Apache Kafka


Setting up kafka:
https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/

Video Kafka Kubernetes: https://www.youtube.com/watch?v=hB6BrhFZs5k&ab_channel=Devoxx

Metrics for Kafka: 
https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/


