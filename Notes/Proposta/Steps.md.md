# Overview

The purpose of this proposal is to provide a solution for a dynamically scaling system, that can respond to a higher/lower demand of data ingestion for the ETL process running by the Data Engineering team. 

## Requirements

1. To consume data in near real-time
2. Reliability
3. Monitoring
4. Compliance with Microservices

## Context

Currently, there is a single consumer for each topic of interest in the Kafka environment. The problem with this solution is that there is no awareness as to what the state of the topic is w.r.t. the amount of data to be read, and the the speed of data consumption is limited by the speed of the single consumer.

Making use of Kafka's consumer group feature, it already allows for a distributed set of consumers to reliably consume information from different partitions, allowing the load to be split by all the consumers in a given consumer group, by dividing the number of partitions by the number of consumers in a group. The number of consumers is always limited by the amount of partitions, as there can't be more than one consumer assigned to a single partition.

Only taking into consideration the real-time data consumption requirement, a good alternative would be to provide with as many consumers as partitions, resulting in the fastest method of consuming the records being published to the Kafka topic. This of course, comes with a cost, as there is always a price to sustain a process, and if the speed at which data is being published into the topic does not justify the speed of consumption, the job being done by all these consumers could be done by a single one at some times.

We arrive at the high level solution, which indicates that there is a need to monitor the state of the kafka topics of interest, to understand how many consumers are needed to satisfy our near-real time requirements based on the amount and speed of the data being inserted/consumed into/from the kafka topic, attempting to optimize the price of running a certain amount of consumers.

## Proposal

Kubernetes is a framework which allows for a good management of computer resources, simplifying the deployment of our services and allowing for autoscaling.

Currently kubernetes already comes with automatic scaling using metrics that are described in each pod. The problem with this implementation is that as measured in the initial phase of the research, usually resources like memory and cpu, are not the limiting factors of the services. And so, we arrive at kubernetes autoscaling using external/custom metrics. 

There are already a few frameworks implemented by other companies that allow for this kind of scaling with technologies and metrics, but most of them do not take into consideration the cost of deployment, contrary to what is needed.

As such, these frameworks make use of Kubernetes REST API, which allows for systems to communicate with the cluster, and deploy/manage our applications, with simple HTTP Requests.

As can be seen in the figure, there are 3 main parts to the solution: 
1. Monitor; 
2. Deployments (Consumer Groups); 
3. Kafka;

### Monitor

To autoscale metrics based on group performance, a Microservice is created which keeps track of a few predefined metrics, which will allow it to determine the amount of consumers each consumer group will require. 

This process would be responsible for polling the required services for the relevant metrics, and to increase the amount of pods in each deployment, based on the information being tracked.

For context, from here onwards, we will use the following terminology, to simplify the mathematical equations.


| Notation     | Description                                                                                              |
| ------------ | -------------------------------------------------------------------------------------------------------- |
| $M^p$        | Set of messages in partition p                                                                           |
| $\epsilon_p$ | The consumer offset for the partition                                                                    |
| $P_i^t$      | Partition i of a topic t                                                                                 |
| $lag_p$      | Difference from the last published message to the last message read from partition p                     |
| $Ct_p$       | Consumption throughput for the last 5 messages in partition p                                            |
| $Pt_p$       | Production throughput for the previous 5 messages in partition p                                         |
| $\rho$       | $max\{\vert M_p \vert, 5\}$                                                                              |
| $M^\rho_p$   | $\rho$ dimensional set of the last $\rho$ messages                                                       |
| $B_m$        | bytes in a message $\forall \;m \in \{1, ..., \vert M \vert\}$                                           |
| $t_m$        | time at which a commit is received at the broker for message $\forall \;m \in \{1, ..., \vert M \vert\}$ |
|              |                                                                                                          |


#### Assumptions

For the following problems described, it is assumed that a single group is connected to a given topic, and that each group is only tracking 1 topic.


#### Formulas

The following equations are proposed, to evaluate the performance of a deployment (consumer group).

$$
	lag_p = \vert M_p \vert - \epsilon_p, \; p \in P_i^t
$$

$$
	Ct^p_i = B_m
$$

$$
	Ct^p = \frac{
		\sum_{m \in \{\vert M \vert - \rho, ..., \vert M \vert\}} B_m
	}{
		t_{|M|} - t_{|M|-\rho|}
	}
$$
soluçao a implementar
	tipo do consumer c/ e sem registry
	
O que testamos do kafka connect
	O que é o registry e porque precisamos de um?
	
kubernetes metal, eks