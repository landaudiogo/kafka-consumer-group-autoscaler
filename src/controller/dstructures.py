import functools
import bisect

from typing import List


class TopicPartitionConsumer:


    def __init__(self, topic, partition, speed=-1): 
        self.speed = speed
        self.partition = partition
        self.topic = topic

    def __hash__(self): 
        return hash((self.topic, self.partition))

    def __eq__(self, other): 
        if isinstance(other, self.__class__):
            return (self.topic, self.partition) == (other.topic, other.partition)
        return False

    def __repr__(self): 
        return f'<< {self.topic}, {self.partition} >> -> {self.speed}'

    def copy(self):
        return TopicPartitionConsumer(self.topic, self.partition)

    def update_speed(self, value): 
        self.speed = value


class PartitionSet(dict):


    def __init__(self, partition_iter: List[TopicPartitionConsumer] = []): 
        partition_dict = {
            tp: tp
            for tp in partition_iter
        }
        super().__init__(**partition_dict)

    def __or__(self, other):
        return PartitionSet( set(self.values()) | set(other.values()) )

    def __sub__(self, other): 
        return ParitionSet( set(self.values()) - set(other.values()) )
    
    def get(self, tp: TopicPartitionConsumer): 
        return super().get(tp)

    def add_partition(self, tp): 
        self[tp] = tp

    def copy(self): 
        return PartitionSet([
            tp.copy() for tp in partition_dict.values()
        ])

    def __repr__(self):
        partition_set = {key for key in self}
        return f'{partition_set}'

class TopicConsumer: 


    def __init__(self, topic_class, topic_name, partitions: List[TopicPartitionConsumer] = []): 
        self.topic_class = {}
        self.topic_name = topic_name
        self.partitions = PartitionSet(partitions)
        self.combined_speed = functools.reduce(
            lambda tp, accum: accum + tp.speed, 
            partitions, 0
        )

    def update_partition_speed(self, partition, value):
        partition = self.partitions.get(partition) 
        if partition == None:
            raise Exception()
        self.combined_speed -= partition.speed
        partition.update_speed(value)
        self.combined_speed += partition.speed

    def add_partition(self, topic_partition: TopicPartitionConsumer):
        self.partitions.add_partition(topic_partition)
        self.combined_speed += topic_partition.speed

    def copy(self): 
        pass

    def __repr__(self): 
        d = {self.topic_name: [partition for partition in self.partitions]}
        return f'{d}'



class TopicDictConsumer(dict):


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __sub__(self, other):
        ret = TopicDictConsumer()
        for key, value in self.items():
            ret[key] = value.copy()
            if other.get(key) != None:
                ret[key].partitions = ret[key].partitions - other[key].partitions
        return ret

    def __or__(self, other): 
        ret = TopicDictConsumer()
        for key, value in self.items(): 
            ret[key] = value.copy()
        for key, value in other.items():
            if ret.get(key) != None:
                ret[key].partitions = ret[key].partitions | value.partitions
            else: 
                ret[key] = value.copy()
        return ret

    def copy(self): 
        pass


class DataConsumer:
    

    def __init__(self, consumer_id): 
        self.consumer_id = consumer_id
        self.assignment = TopicDictConsumer()
        self.combined_speed = 0

    def update_partition_speed(self, partition, value): 
        topic = self.assignment.get(partition.topic)
        if topic == None:
            raise Exception()
        self.combined_speed -= topic.combined_speed
        topic.update_partition_speed(partition, value)
        self.combined_speed += topic.combined_speed

    def add_partition(self, partition: TopicPartitionConsumer): 
        topic = self.assignment.get(partition.topic)
        if topic == None: 
            topic = TopicConsumer(partition.topic)
            self.assignment[partition.topic] = topic
        self.combined_speed -= topic.combined_speed
        topic.add_partition(partition)
        self.combined_speed += topic.combined_speed


class ConsumerList(list):
    """Mapping to keep track of the consumers that are in use.

    This class maintains the list's functionalities, and extends it's use to the
    specific use case of the list of consumers. 
    """


    def __init__(self): 
        super().__init__()
        self.available_indices = []

    def create_bin(self, idx: int = None):
        """Creates a new consumer in the existing list.

        If the idx is provided, the consumer will be created at that index. In
        the case the idx provided exceeds the current size of the list, the
        elements in between the last element and idx are assigned None. 

        Args: 
            idx: index at which the consumer is to be created

        Returns: 
            None

        Raises: 
        """
        last_idx = len(self) - 1
        if idx == None: 
            if len(self.available_indices): 
                lowest_idx = self.available_indices[0]
                self[lowest_idx] = DataConsumer(lowest_idx)
                self.available_indices.pop(0)
            else: 
                self.append(DataConsumer(last_idx+1))
        else:
            if (idx > last_idx): 
                for i in range(idx-last_idx-1): 
                    self.append(None)
                    self.available_indices.append(last_idx+i+1)
                self.append(DataConsumer(idx))
            else:
                if (self[idx] != None): 
                    raise Exception()
                pos = bisect.bisect_left(self.available_indices, idx)
                if ((pos == len(self.available_indices))
                    or (self.available_indices[pos] != idx) 
                ):
                    raise Exception()
                self[idx] = DataConsumer(idx)
                self.available_indices.pop(pos)


    def remove_bin(self, idx): 
        last_idx = len(self) - 1
        if (idx > last_idx) or (self[idx] == None): 
            raise Exception()
        if idx == last_idx: 
            self.pop(-1)
            while(len(self) and (self[-1] == None)): 
                self.pop(-1)
                self.available_indices.pop(-1)
        else:
            self[idx] = None
            bisect.insort(self.available_indices, idx)

    def __sub__(self, other): 
        pass

