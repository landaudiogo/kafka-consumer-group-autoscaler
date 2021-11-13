import itertools
import functools
import bisect

from typing import List, Iterable, Union, Type, Optional
from config import CONSUMER_CAPACITY, ALGO_CAPACITY, DETopicMetadata



@functools.total_ordering
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

    def __lt__(self, other):
        if not isinstance(other, TopicPartitionConsumer): 
            raise Exception()
        return self.speed < other.speed

    def __repr__(self): 
        return f'<{self.partition} -> {self.speed}>'

    def copy(self):
        return TopicPartitionConsumer(
            self.topic, self.partition, speed=self.speed
        )

    def update_speed(self, value): 
        self.speed = value


class PartitionSet(dict):


    def __init__(self, partition_iter: Iterable[TopicPartitionConsumer] = None): 
        partition_iter = partition_iter if partition_iter != None else []
        partition_dict = {
            tp: tp
            for tp in partition_iter
        }
        super().__init__(partition_dict)

    def __or__(self, other: "PartitionSet"):
        return PartitionSet( set(self.values()) | set(other.values()) )

    def __sub__(self, other: "PartitionSet"): 
        return PartitionSet( set(self.values()) - set(other.values()) )
    
    def add_partition(self, tp: TopicPartitionConsumer): 
        self[tp] = tp

    def copy(self): 
        return PartitionSet([tp.copy() for tp in self])

    def __repr__(self):
        partition_set = {key for key in self}
        return f'{partition_set}'

    def to_list(self): 
        return [tp for tp in self]


class TopicConsumer: 


    def __init__(
        self, 
        topic_name, 
        partitions: Iterable[TopicPartitionConsumer] = None,
    ):
        if partitions == None: 
            partitions = []
        topic_metadata = DETopicMetadata[topic_name]
        self.topic_class = topic_metadata["topic_class"]
        self.bq_table = topic_metadata["bq_table"]
        self.topic_name = topic_name
        self.partitions = PartitionSet(partitions)
        self.combined_speed = functools.reduce(
            lambda accum, tp: accum + tp.speed, 
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

    def remove_partition(self, tp: TopicPartitionConsumer): 
        if self.partitions.get(tp) == None: 
            return 
        tp = self.partitions.pop(tp)
        self.combined_speed -= tp.speed

    def copy(self): 
        return TopicConsumer(
            self.topic_name,
            self.partitions.copy().to_list(),
        )

    def __repr__(self): 
        d = {self.topic_name: [partition for partition in self.partitions]}
        return f'{d}'

    def to_record(self): 
        return {
            "topic_name": self.topic_name,
            "topic_class": self.topic_class, 
            "partitions": [tp.partition for tp in self.partitions.to_list()],
            "bq_table": self.bq_table,
            "ignore_events": [],
        }


class TopicDictConsumer(dict):


    def __init__(self, headers=None, **kwargs):
        if headers != None:
            self.headers = headers
        super().__init__(kwargs)

    def __sub__(self, other):
        if not isinstance(other, TopicDictConsumer): 
            raise Exception()
        ret = TopicDictConsumer()
        for key, value in self.items():
            ret[key] = value.copy()
            if other.get(key) != None:
                ret[key].partitions = ret[key].partitions - other[key].partitions
        return ret

    def __or__(self, other): 
        if not isinstance(other, TopicDictConsumer): 
            raise Exception()
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
        return TopicDictConsumer(
            **{key: value.copy() for key, value in self.items()}
        )

    def __repr__(self): 
        return f'{set(self.values())}'

    def partitions(self): 
        res = PartitionSet()
        for v in self.values(): 
            res = res | v.partitions
        return res

    def to_record(self): 
        return {
            "headers": self.headers, 
            "payload": [
                value.to_record() 
                for value in self.values()
            ]
        }

    def __eq__(self, other): 
        if not isinstance(other, TopicDictConsumer): 
            return False
        if len(self) != len(other): 
            return False
        for key, vref in self.items(): 
            vcmp = other.get(key)
            if vcmp == None: 
                return False
            if vcmp != vref: 
                return False
        return True

@functools.total_ordering
class DataConsumer: 

    def __init__(
            self, 
            consumer_id, 
            assignment: TopicDictConsumer = None
        ): 
        self.consumer_id = consumer_id
        self.assignment = assignment if assignment != None else TopicDictConsumer()
        self.combined_speed = functools.reduce(
            lambda accum, topic: accum + topic.combined_speed,
            self.assignment.values(), 0
        )

    def __hash__(self):
        return hash(self.consumer_id)

    def __eq__(self, other):
        if not isinstance(other, DataConsumer): 
            return False
        return self.consumer_id == other.consumer_id

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

    def fits(self, tp: TopicPartitionConsumer): 
        return (
            True if (self.combined_speed + tp.speed < ALGO_CAPACITY) 
            else False
        )

    def __repr__(self): 
        return f'{self.consumer_id}'

    def __lt__(self, other: Optional['DataConsumer']): 
        if other == None: 
            return False
        if not isinstance(other, DataConsumer): 
            raise Exception()
        return self.combined_speed < other.combined_speed

    def partitions(self): 
        return self.assignment.partitions()

    def __sub__(self, other):
        if self.consumer_id != other.consumer_id: 
            raise Exception()
        return DataConsumer(self.consumer_id, assignment=self.assignment-other.assignment)


class ConsumerList(list):
    """Mapping to keep track of the consumers that are in use.

    This class maintains the list's functionalities, and extends it's use to the
    specific use case of the list of consumers. 
    """


    def __init__(self, clist: Optional[List[DataConsumer]] = None): 
        super().__init__()
        if clist == None: 
            clist = []
        self.available_indices = []
        self.map_partition_consumer = {}
        for i, c in enumerate(clist):
            if c == None: 
                self.available_indices.append(i)
                continue
            for tp in c.partitions():
                self.map_partition_consumer[tp] = c
        super().__init__(clist)


    def create_bin(self, idx: Optional[int] = None):
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
                return self.available_indices.pop(0)
            else: 
                self.append(DataConsumer(last_idx+1))
                return last_idx+1
        else:
            if (idx < 0): 
                raise Exception()
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

    def get_idx(self, idx: int): 
        if (-len(self) <= idx < len(self)): 
            return self[idx]
        return None

    def remove_bin(self, idx: int): 
        last_idx = len(self) - 1
        if ((idx > last_idx) or (idx < 0)) or (self[idx] == None): 
            raise Exception()
        if idx == last_idx: 
            self.pop(-1)
            while(len(self) and (self[-1] == None)): 
                self.pop(-1)
                self.available_indices.pop(-1)
        else:
            self[idx] = None
            bisect.insort(self.available_indices, idx)

    def assign_partition_consumer(self, idx, tp): 
        if((idx >= len(self)) or (idx < -len(self))): 
            raise Exception()
        if(self[idx] == None): 
            raise Exception()
        self[idx].add_partition(tp)
        self.map_partition_consumer[tp] = self[idx]

    def get_consumer(self, tp: TopicPartitionConsumer):
        return self.map_partition_consumer.get(tp)

    def __sub__(self, other): 
        gm = GroupManagement()
        for i, (final, current) in enumerate(itertools.zip_longest(self, other)):
            if (final, current) == (None, None): 
                continue
            if final == None: 
                final = DataConsumer(i)
                gm.add_consumers_remove(final)
            if current == None: 
                current = DataConsumer(i)
                gm.add_consumers_create(current)
            start = final - current
            stop = current - final
            for partition in start.partitions():
                action = StartCommand(final, partition)
                gm.add_action(action)
            for partition in stop.partitions():
                action = StopCommand(final, partition)
                gm.add_action(action)
        return gm

    def partitions(self):
        all_partitions = PartitionSet()
        for c in self:
            all_partitions = all_partitions | c.partitions()
        return all_partitions


class Command:
    def __init__(
        self, 
        consumer: DataConsumer, 
        partition: TopicPartitionConsumer
    ): 
        self.consumer = consumer
        self.partition = partition

    def __eq__(self, other): 
        if not isinstance(other, Command): 
            return False
        return (
            (self.consumer, self.partition) == 
            (other.consumer, other.partition)
        )
            

class StopCommand(Command):
    pass


class StartCommand(Command): 
    pass


class Event:
    def __init__(
        self, 
        consumer: DataConsumer, 
        partition: TopicPartitionConsumer
    ):
        self.consumer = consumer
        self.partition = partition


class StopEvent(Event):
    pass


class StartEvent(Event):
    pass


class PartitionCommands:
    """Track what kind of Commands that have to be performed for a partition.

    The 2 attributes define the actions the partition has to go through. 
    """

    def __init__(self, partition: TopicPartitionConsumer, action: Command = None): 
        self.partition = partition
        self.start = None
        self.stop = None
        if action != None:
            if action.__class__ == StopCommand:
                self.stop = action
            elif action.__class__ == StartCommand:
                self.start = action

    def __eq__(self, other): 
        if not isinstance(other, PartitionCommands): 
            return False
        return (
            (self.partition, self.start, self.stop) == 
            (other.partition, other.start, other.stop)
        )

    def add_action(self, action: Command): 
        if not action.partition == self.partition: 
            raise Exception()
        if action.__class__ == StartCommand:
            self.start = action
        elif action.__class__ == StopCommand:
            self.stop = action
        else:
            raise Exception()

    def remove_action(self, action: Command):
        if not action.partition == self.partition: 
            raise Exception()

        if action.__class__ == StartCommand:
            self.start = None
        elif action.__class__ == StopCommand:
            self.stop = None

    def empty():
        return (self.start, self.stop) == (None, None)

class GroupManagement:


    def __init__(self): 
        self.batch = ConsumerMessageBatch()
        self.map_partition_actions = {}
        self.consumers_create = set()
        self.consumers_remove = set()

    def add_action(self, action: Command): 
        p_actions = self.map_partition_actions.get(action.partition)
        if p_actions == None:
            p_actions = PartitionCommands(action.partition)
            self.map_partition_actions[action.partition] = p_actions

        if action.__class__ == StopCommand:
            if p_actions.start != None:
                self.batch.remove_action(action)
            self.batch.add_action(action)
        elif action.__class__ == StartCommand: 
            if p_actions.stop == None:
                self.batch.add_action(action)
        p_actions.add_action(action)

    def remove_action(self, event: Event):
        p_actions = self.map_partition_actions.get(event.partition)
        if p_actions == None:
            raise Exception()

        if isinstance(event, StopEvent):
            p_actions.remove_action(event)
            if p_actions.start != None: 
                self.batch.add_action(p_actions.start)
        elif isinstance(event, StartEvent):
            p_actions.remove_action(event)

        if p_actions.empty(): 
            self.map_partition_actions.pop(event.partition)

    def prepare_batch(self, event: Event):
        for topic_partitions in event:
            print(topic_partitions["topic_name"], topic_partitions["partitions"])

    def add_consumers_remove(self, consumer: DataConsumer): 
        self.consumers_remove.add(consumer)

    def add_consumers_create(self, consumer: DataConsumer):
        self.consumers_create.add(consumer)

    def pop_consumers_remove(self, consumer: DataConsumer): 
        self.consumers_remove.remove(consumer)

    def pop_consumers_create(self, consumer: DataConsumer):
        self.consumers_create.remove(consumer)



class ConsumerMessageBatch(dict):
    """This data structure aims to prepare a Batch of messages to be sent at
    once.

    The key represents a single instance of type DataConsumer, and the value is an instance of type
    ConsumerMessage, which can contain messages of type StartCommand or StopCommand Consuming
    Commands for the Consumer.
    """


    def add_action(self, action: Command): 
        cmsg = self.get(action.consumer)
        if cmsg == None: 
            cmsg = ConsumerMessage(action.consumer)
            self[action.consumer] = cmsg
        cmsg.add_action(action)


class ConsumerMessage:
    """Stores the start and stop messages directed for a single consumer."""


    def __init__(self, consumer: DataConsumer): 
        self.consumer = consumer
        self.start = TopicDictConsumer(
            headers={
                "event_type":"StartConsumingCommand",
                "serializer": "de_avro.DEControllerSchema",
            }
        )
        self.stop = TopicDictConsumer(
            headers={
                "event_type":"StopConsumingCommand",
                "serializer": "de_avro.DEControllerSchema",
            }
        )


    def add_action(self, action: Command): 
        if action.__class__ == StartCommand: 
            topic = self.start.get(action.partition.topic)
            if topic == None:
                topic = TopicConsumer(action.partition.topic)
                self.start[action.partition.topic] = topic
            topic.add_partition(action.partition)
        elif action.__class__ == StopCommand: 
            topic = self.stop.get(action.partition.topic)
            if topic == None:
                topic = TopicConsumer(action.partition.topic)
                self.stop[action.partition.topic] = topic
            self.stop[action.partition.topic].add_partition(action.partition)

    def remove_action(self, action: Command): 
        if action.__class__ == StartCommand: 
            topic = self.start.get(action.partition.topic)
            if topic == None:
                return 
            topic.remove_partition(action.partition)
        elif action.__class__ == StopCommand: 
            topic = self.stop.get(action.partition.topic)
            if topic == None:
                return 
            topic.remove_partition(action.partition)

    def to_record_list(self): 
        l = []
        if self.start != TopicDictConsumer():
            l.append(self.start.to_record())
        if self.stop != TopicDictConsumer():
            l.append(self.stop.to_record())
        return l

