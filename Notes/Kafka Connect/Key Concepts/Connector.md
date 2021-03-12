# Connector

Does not perform data copying, but via it's configuration, describes the set of data to be copied, and the conector is responsible for breaking the job into a set of tasks that can be assigned to kafka connect workers.

Can also be responsible for monitoring the data changes of the external systems, and request task reconfiguration. 

