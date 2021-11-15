from dstructures import (
    ConsumerList, TopicPartitionConsumer, DataConsumer
)


class ApproximationAlgorithm: 


    def run(self, cpartitions, unassigned): 
        for tp in cpartitions:
            self.assign(tp) 
        for tp in unassigned:
            self.assign(tp) 
        print(self.next_assignment)
        return self.next_assignment

    def assign(self): 
        pass



class NextFit(ApproximationAlgorithm): 


    def run(self, consumer_list, unassigned): 
        self.next_assignment = ConsumerList()
        self.next_assignment.create_bin()
        return super().run(consumer_list.partitions(), unassigned)

    def assign(self, tp: TopicPartitionConsumer): 
        print(tp, self.next_assingment[-1])
        print(self.next_assignment[-1].fits(tp))
        print(self.next_assignment[-1].combined_speed)
        if (
            (not self.next_assignment[-1].fits(tp)) 
            and (self.next_assignment[-1].combined_speed != 0)
        ): 
            self.next_assignment.create_bin()
        self.next_assignment.assign_partition_consumer(-1, tp)


class NextFitDecreasing(NextFit): 


    def run(self, consumer_list, unassigned): 
        cpartitions = consumer_list.partitions()
        clist = ConsumerList(sorted(cpartitions, reverse=True))
        unass = sorted(unassigned, reverse=True)
        return super().run(clist, unass)


class FirstFit(ApproximationAlgorithm): 


    def run(self, consumer_list, unassigned):
        self.next_assignment = ConsumerList()
        return super().run(consumer_list.partitions(), unassigned)

    def assign(self, tp: TopicPartitionConsumer): 
        firstfit = None
        for c in self.next_assignment: 
            if c.fits(tp): 
                firstfit = c
                break
        idx = (
            self.next_assignment.create_bin() if firstfit == None
            else firstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class FirstFitDecreasing(FirstFit): 


    def run(self, consumer_list, unassigned):
        cpartitions = consumer_list.partitions()
        clist = ConsumerList(sorted(cpartitions, reverse=True))
        unass = sorted(unassigned, reverse=True)
        return super().run(clist, unass)


class WorstFit(ApproximationAlgorithm): 

    
    def run(self, consumer_list, unassigned): 
        self.next_assignment = ConsumerList()
        return super().run(consumer_list.partitions(), unassigned)

    def assign(self, tp: TopicPartitionConsumer): 
        worstfit = None
        for c in self.next_assignment: 
            if c == None: 
                continue
            if c.fits(tp): 
                if (
                    (worstfit == None) 
                    or (c.combined_speed > worstfit.combined_speed)
                ):
                    worstfit = c
        idx = (
            self.next_assignment.create_bin() if worstfit == None
            else worstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class WorstFitDecreasing(WorstFit): 

    
    def run(self, consumer_list, unassigned): 
        cpartitions = consumer_list.partitions()
        clist = ConsumerList(sorted(cpartitions, reverse=True))
        unass = sorted(unassigned, reverse=True)
        return super().run(clist, unass)


class BestFit(ApproximationAlgorithm): 


    def run(self, consumer_list, unassigned):
        self.next_assignment = ConsumerList()
        return super().run(consumer_list.partitions(), unassigned)

    
    def assign(self, tp: TopicPartitionConsumer): 
        bestfit = None
        for c in self.next_assignment: 
            if c.fits(tp):
                if (bestfit == None) or (c.combined_speed > bestfit.combined_speed): 
                    bestfit = c
        idx = (
            self.next_assignment.create_bin() if bestfit == None 
            else bestfit.consumer_id
        ) 
        self.next_assignment.assign_partition_consumer(idx, tp)


class BestFitDecreasing(BestFit): 


    def run(self, consumer_list, unassigned): 
        cpartitions = consumer_list.partitions()
        clist = ConsumerList(sorted(cpartitions, reverse=True))
        unass = sorted(unassigned, reverse=True)
        return super().run(clist, unass)


class ModifiedWorstFit(WorstFit): 


    def run(self, consumer_list, unassigned): 
        self.next_assignment = ConsumerList()
        clist = [consumer for consumer in consumer_list if consumer != None]
        clist = sorted(clist, reverse=True)
        for c in clist: 
            if c == None: 
                continue
            cpartitions = sorted(c.partitions().to_list(), reverse=True)
            for i in range(len(cpartitions)-1, -1, -1):
                tp = cpartitions[i]
                res = self.assign_existing(tp)
                if res == False:
                    break
                cpartitions.pop(i)
            if len(cpartitions):
                for j, tp in enumerate(cpartitions):
                    res = self.assign_current_consumer(tp, c)
                    if res == False: 
                        break
                if (j <= len(cpartitions)-1) and (res == False):
                    unassigned.extend(cpartitions[j:])

        unassigned = sorted(unassigned, reverse=True)
        for tp in unassigned: 
            self.assign(tp)
        print(self.next_assignment)
        return self.next_assignment

    def assign_existing(self, tp: TopicPartitionConsumer): 
        worstfit = None
        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                if (
                    (worstfit == None)
                    or (c.combined_speed < worstfit.combined_speed)
                ):
                    worstfit = c
        if worstfit != None: 
            self.next_assignment.assign_partition_consumer(
                worstfit.consumer_id, tp
            )
            return True
        return False

    def assign_current_consumer(
        self, 
        tp: TopicPartitionConsumer, 
        consumer: DataConsumer,
    ):
        idx = consumer.consumer_id
        c = self.next_assignment.get_idx(idx)
        if c == None:
            self.next_assignment.create_bin(idx)
            c = self.next_assignment[idx]
        if c.fits(tp):
            self.next_assignment.assign_partition_consumer(idx, tp)
            return True
        return False



class AlgorithmFactory:


    @classmethod
    def get_algorithm(self, algo_name): 
        algo_name = algo_name.lower()
        if algo_name in ("nfd", "next fit decreasing"): 
            return NextFitDecreasing()
        elif algo_name in ("nf", "next fit"): 
            return NextFit()
        elif algo_name in ("bf", "best fit"): 
            return BestFit()
        elif algo_name in ("bfd", "best fit decreasing"): 
            return BestFitDecreasing()
        elif algo_name in ("wf", "worst fit"):
            return WorstFit()
        elif algo_name in ("wfd", "worst fit decreasing"):
            return WorstFitDecreasing()
        elif algo_name in ("ff", "first fit"): 
            return FirstFit()
        elif algo_name in ("ffd", "first fit decreasing"): 
            return FirstFitDecreasing()
        elif algo_name in ("mwf", "modified worst fit"):
            return ModifiedWorstFit()
        else: 
            raise Exception()
