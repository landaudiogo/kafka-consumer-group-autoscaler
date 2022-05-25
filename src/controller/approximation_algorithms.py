import random

from dstructures import (
    ConsumerList, TopicPartitionConsumer, DataConsumer, PartitionSet
)
from exc import UndefinedAlgorithm


class ApproximationAlgorithm: 


    def run(self): 
        for tp in self.partitions:
            self.assign(tp) 
        return self.next_assignment

    def treat_input(self, consumer_list, unassigned): 
        self.partitions = list(consumer_list.partitions() | PartitionSet(unassigned))
        random.shuffle(self.partitions)
        self.consumer_list = consumer_list


class DecreasingInput:


    def treat_input(self, consumer_list, unassigned): 
        pset = consumer_list.partitions() | PartitionSet(unassigned)
        self.partitions = sorted(pset.to_list(), reverse=True)
        self.consumer_list = consumer_list


class NextFit(ApproximationAlgorithm): 


    def run(self):
        self.next_assignment = ConsumerList()
        self.next_assignment.create_bin()
        return super().run()

    def assign(self, tp: TopicPartitionConsumer): 
        create_current = None
        current = self.consumer_list.get_consumer(tp)
        if current != None:
            if self.next_assignment.get_idx(current.consumer_id) == None: 
                create_current = current.consumer_id
            else: 
                create_current = None

        if (
            (not self.next_assignment.last_created_bin.fits(tp)) 
            and (self.next_assignment.last_created_bin.combined_speed != 0)
        ): 
            self.next_assignment.create_bin(idx=create_current)
        idx = self.next_assignment.last_created_bin.consumer_id
        self.next_assignment.assign_partition_consumer(idx, tp)


class NextFitDecreasing(DecreasingInput, NextFit): 
        pass


class FirstFit(ApproximationAlgorithm): 


    def run(self):
        self.next_assignment = ConsumerList()
        return super().run()

    def assign(self, tp: TopicPartitionConsumer): 
        firstfit = None
        create_current = None

        current = self.consumer_list.get_consumer(tp)
        if current != None:
            if self.next_assignment.get_idx(current.consumer_id) == None: 
                create_current = current.consumer_id
            else: 
                create_current = None

        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                firstfit = c
                break

        idx = (
            self.next_assignment.create_bin(idx=create_current) if firstfit == None
            else firstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class FirstFitDecreasing(DecreasingInput, FirstFit): 
    pass


class WorstFit(ApproximationAlgorithm): 

    def run(self): 
        self.next_assignment = ConsumerList()
        return super().run()

    def assign(self, tp: TopicPartitionConsumer): 
        worstfit = None
        create_current = None

        current = self.consumer_list.get_consumer(tp)
        if current != None:
            if self.next_assignment.get_idx(current.consumer_id) == None: 
                create_current = current.consumer_id
            else: 
                create_current = None

        for c in self.next_assignment: 
            if c == None: 
                continue
            if c.fits(tp): 
                if (
                    (worstfit == None) 
                    or (c.combined_speed < worstfit.combined_speed)
                ):
                    worstfit = c

        idx = (
            self.next_assignment.create_bin(idx=create_current) if worstfit == None
            else worstfit.consumer_id
        )
        self.next_assignment.assign_partition_consumer(idx, tp)


class WorstFitDecreasing(DecreasingInput, WorstFit): 
    pass


class BestFit(ApproximationAlgorithm): 


    def run(self):
        self.next_assignment = ConsumerList()
        return super().run()
    
    def assign(self, tp: TopicPartitionConsumer): 
        bestfit = None
        create_current = None

        current = self.consumer_list.get_consumer(tp)
        if current != None:
            if self.next_assignment.get_idx(current.consumer_id) == None: 
                create_current = current.consumer_id
            else: 
                create_current = None

        for c in self.next_assignment: 
            if c == None:
                continue
            if c.fits(tp): 
                if (bestfit == None) or (c.combined_speed > bestfit.combined_speed): 
                    bestfit = c

        idx = (
            self.next_assignment.create_bin(idx=create_current) if bestfit == None 
            else bestfit.consumer_id
        ) 
        self.next_assignment.assign_partition_consumer(idx, tp)


class BestFitDecreasing(DecreasingInput, BestFit): 
    pass


class ModifiedWorstFit(WorstFit): 
    

    def __init__(self, compare_key=None):
        self.compare_key = compare_key

    def treat_input(self, consumer_list, unassigned):
        self.consumer_list = consumer_list
        self.unassigned = unassigned

    def run(self): 
        consumer_list = self.consumer_list
        unassigned = self.unassigned
        self.next_assignment = ConsumerList()

        clist = [consumer for consumer in consumer_list if consumer != None]
        clist = sorted(clist, reverse=True, key=self.compare_key)
        for c in clist: 
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



class ModifiedBestFit(BestFit): 


    def __init__(self, compare_key=None):
        self.compare_key = compare_key

    def treat_input(self, consumer_list, unassigned):
        self.consumer_list = consumer_list
        self.unassigned = unassigned

    def run(self): 
        consumer_list = self.consumer_list
        unassigned = self.unassigned
        self.next_assignment = ConsumerList()

        clist = [consumer for consumer in consumer_list if consumer != None]
        clist = sorted(clist, reverse=True, key=self.compare_key)
        for c in clist: 
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
                    or (c.combined_speed > bestfit.combined_speed)
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
            return ModifiedBestFit()
        elif algo_name in ("mwfp", "modified worst fit partitions"):
            return ModifiedWorstFit(compare_key=DataConsumer.biggest_speed)
        elif algo_name in ("mbfp", "modified best fit partitions"):
            return ModifiedBestFit(compare_key=DataConsumer.biggest_speed)
        else: 
            raise UndefinedAlgorithm(f"{algo_name} is not a defined Algorithm")
