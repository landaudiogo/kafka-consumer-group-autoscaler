# Workers

Connector's and tasks are logical units of work, and have to be executed in a process. These processes are called **Workers**. 

## Distributed Workers: 

In this mode, several workers are started with the same `group.id` and they automatically coordinate to schedule execution of connectors and tasks across all workers.


