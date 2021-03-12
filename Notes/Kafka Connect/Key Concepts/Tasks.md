# Tasks

Allows for a distributed running mode, providing scalability, and state is stored not within the tasks, but in kafka in the following topics: `status.storage.topic` and `config.storage.topic`.

Similar to a consumer group, when 2 connectors connect to the same `group.id`, the tasks and connectors associated to the consumer group are rebalanced between all the members of the cluster.

**Rebalancing** is triggered when: 
	- connector's increase or decrease the amount of tasks they require
	- connector's configuration file is changed
	- When the amount of workers increases or decreases