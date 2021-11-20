from dstructures import (
    ConsumerList, TopicPartitionConsumer, DataConsumer, PartitionSet
)
import random


class ApproximationAlgorithm: 


    def run(self): 
        for tp in self.partitions:
            self.assign(tp) 
        return self.next_assignment

    def assign(self): 
        pass



class NextFit(ApproximationAlgorithm): 

    def run(self):
        self.next_assignment = ConsumerList()
        self.next_assignment.create_bin()
        return super().run()

    def treat_input(self, consumer_list, unassigned):
        self.partitions = PartitionSet(unassigned) | consumer_list.partitions()
        self.partitions = list(self.partitions)
        random.shuffle(self.partitions)
        self.next_assignment = ConsumerList()
        self.next_assignment.create_bin()

    def assign(self, tp: TopicPartitionConsumer): 
        if (
            (not self.next_assignment[-1].fits(tp)) 
            and (self.next_assignment[-1].combined_speed != 0)
        ): 
            self.next_assignment.create_bin()
        self.next_assignment.assign_partition_consumer(-1, tp)


class NextFitDecreasing(NextFit): 

    def treat_input(self, consumer_list, unassigned):
        pset = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = sorted(pset.to_list(), reverse=True)



class FirstFit(ApproximationAlgorithm): 


    def run(self):
        self.next_assignment = ConsumerList()
        return super().run()

    def treat_input(self, consumer_list, unassigned): 
        self.consumer_list = consumer_list
        self.partitions = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = list(self.partitions)
        random.shuffle(self.partitions)

    def assign(self, tp: TopicPartitionConsumer): 
        firstfit = None
        c_consumer_idx = self.consumer_list.get_consumer(tp)
        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                firstfit = c
                break

        if c_consumer_idx != None:
            c_consumer_idx = (c_consumer_idx.consumer_id
                if self.next_assignment.get_idx(c_consumer_idx.consumer_id) == None
                else None
            )
        idx = (
            self.next_assignment.create_bin(idx=c_consumer_idx) if firstfit == None
            else firstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class FirstFitDecreasing(FirstFit): 

    def treat_input(self, consumer_list, unassigned): 
        pset = consumer_list.partitions() | PartitionSet(unassigned)
        self.consumer_list = consumer_list
        self.partitions = sorted(pset.to_list(), reverse=True)


class WorstFit(ApproximationAlgorithm): 

    def treat_input(self, consumer_list, unassigned): 
        self.partitions = PartitionSet(unassigned) | consumer_list.partitions()
        self.partitions = list(self.partitions)
        random.shuffle(self.partitions)
        self.consumer_list = consumer_list
    
    def run(self): 
        self.next_assignment = ConsumerList()
        return super().run()

    def assign(self, tp: TopicPartitionConsumer): 
        worstfit = None
        c_consumer_idx = self.consumer_list.get_consumer(tp)
        for c in self.next_assignment: 
            if c == None: 
                continue
            if c.fits(tp): 
                if (
                    (worstfit == None) 
                    or (c.combined_speed > worstfit.combined_speed)
                ):
                    worstfit = c

        if c_consumer_idx != None:
            c_consumer_idx = (c_consumer_idx.consumer_id
                if self.next_assignment.get_idx(c_consumer_idx.consumer_id) == None
                else None
            )
        idx = (
            self.next_assignment.create_bin() if worstfit == None
            else worstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class WorstFitDecreasing(WorstFit): 

    
    def treat_input(self, consumer_list, unassigned):
        pset = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = sorted(pset.to_list(), reverse=True)
        self.consumer_list = consumer_list


class BestFit(ApproximationAlgorithm): 


    def treat_input(self, consumer_list, unassigned): 
        self.partitions = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = list(self.partitions)
        random.shuffle(self.partitions)
        self.consumer_list = consumer_list

    def run(self):
        self.next_assignment = ConsumerList()
        return super().run()
    
    def assign(self, tp: TopicPartitionConsumer): 
        bestfit = None
        c_consumer_idx = self.consumer_list.get_consumer(tp)
        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                if (bestfit == None) or (c.combined_speed > bestfit.combined_speed): 
                    bestfit = c

        if c_consumer_idx != None:
            c_consumer_idx = (c_consumer_idx.consumer_id
                if self.next_assignment.get_idx(c_consumer_idx.consumer_id) == None
                else None
            )
        idx = (
            self.next_assignment.create_bin(idx=c_consumer_idx) if bestfit == None 
            else bestfit.consumer_id
        ) 
        self.next_assignment.assign_partition_consumer(idx, tp)


class BestFitDecreasing(BestFit): 


    def treat_input(self, consumer_list, unassigned):
        pset = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = sorted(pset.to_list(), reverse=True)
        self.consumer_list = consumer_list


class ModifiedWorstFit(WorstFit): 
    

    def treat_input(self, consumer_list, unassigned):
        self.consumer_list = consumer_list
        self.unassigned = unassigned

    def run(self): 
        consumer_list = self.consumer_list
        unassigned = self.unassigned
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
        return self.next_assignment

    def assign_existing(self, tp: TopicPartitionConsumer): 
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



class ModifiedBestFit(BestFit): 
    

    def treat_input(self, consumer_list, unassigned):
        self.consumer_list = consumer_list
        self.unassigned = unassigned

    def run(self): 
        consumer_list = self.consumer_list
        unassigned = self.unassigned
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
        return self.next_assignment

    def assign_existing(self, tp: TopicPartitionConsumer): 
        bestfit = None
        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                if (
                    (bestfit == None)
                    or (c.combined_speed < bestfit.combined_speed)
                ):
                    bestfit = c
        if bestfit != None: 
            self.next_assignment.assign_partition_consumer(
                bestfit.consumer_id, tp
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
        elif algo_name in ("mbf", "modified best fit"):
            return ModifiedWorstFit()
        else: 
            raise Exception()
