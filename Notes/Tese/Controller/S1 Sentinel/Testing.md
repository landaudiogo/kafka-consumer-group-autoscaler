# Overview

Testing this state involves testing the multiple side effects triggered by this state's execution: 

These are: 
1. Changing a consumer's combined speed when reading the last message.
2. adding partitions to the unassigned if there is no consumer currently assigned to it.


# Add a partition to unassigned

Have the controller not assigned to anything and verify whether the partition is added to the unassigned list. 

Have a consumer be assigned to the partition and verify whether the partition is added to the list.

# Modifying a consumer's combined speed

Have a consumer be assigned to a parititon, and verify whether the consumer's combined_speed is changed.
