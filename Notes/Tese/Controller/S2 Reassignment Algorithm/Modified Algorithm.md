The approximation algorithm rule is the parameter than can vary within this implementation, and is a design chioce. 

The first implementation will make use of the worst fit algorithm to try and balance the load between all consumers.

The algorithm has the following phases:
1. Sort the consumers decreasingly based on their speed.
2. For each consumer in the sorted list, do:
	1. Sort the list of partitions assigned to the consumer from biggest to smallest.
	2. Starting at the end of the sorted list of partitions, try to assign the partition to one of the existing bins through one of the approximation algorithm rules.
		1. If it fits, the partition is removed from the list, and the consumer gets the new partition. Go back to 2.2.
		2. If it does not fit, then we stop this phase and enter phase 2.3. 
	3. Verify if the partition list is empty. 
		1. If it is, then repeat from point 2.
		2. Else, create a new consumer (the consumer the partitions we are currently assigning belong to), and, prioritizing the biggest elements, fill the consumer with as many partitions possible, until the list ends. The elements that remain will be stored in the unassigned partitions for future use. Go back to step 2.
3. Verify whether the unassigned partitons list is empty. 
	1. If it is not empty:
		1. Sort the list of partitions in a decreasing manner.
		2. For each partition in the sorted list, using the same rule as in 2.2, assign the partition to one of the existing consumers, or create a new bin (consumer) to assign the partition to.
		3. Trigger the start of the third state (S3 consumer state management)
	2. Else: 
		1. Trigger the start of the third state (S3 consumer state management)


