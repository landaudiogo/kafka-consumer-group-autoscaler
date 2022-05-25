import itertools
import functools
import bisect
import logging
import sys

from typing import List, Iterable, Union, Type, Optional
from functools import cmp_to_key

from config import CONSUMER_CAPACITY, ALGO_CAPACITY, DETopicMetadata
from exc import (
    ConsumerAlreadyExists, InvalidRange, ParameterException
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
        partition_list = [key for key in self]
        return f'{partition_list}'

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

    @classmethod
    def from_json(cls, topic): 
        topic_name, plist = topic["topic_name"], topic["partitions"]
        plist = [TopicPartitionConsumer(topic_name, p) for p in plist]
        return cls(topic_name, plist)

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
            raise Exception() 
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

    def to_json(self): 
        return {
            "topic_name": self.topic_name,
            "partitions": [p.partition for p in self.partitions.to_list()]
        }


class TopicDictConsumer(dict):


    def __init__(self, headers=None, **kwargs):
        if headers != None:
            self.headers = headers
        super().__init__(kwargs)

    @classmethod
    def from_json(cls, assignment): 
        d = {
            topic["topic_name"]: TopicConsumer.from_json(topic)
            for topic in assignment
        }
        return cls(**d)


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
                    if value.to_record() != None
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

    @classmethod
    def from_json(cls, idx, assignment):
        assignment = TopicDictConsumer.from_json(assignment)
        return cls(idx, assignment=assignment)

    @staticmethod
    @cmp_to_key
    def idx_sort(c1, c2): 
        return c1.biggest_speed() - c2.biggest_speed()

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
        if (
            ((self.combined_speed == 0) and (len(self.partitions()) == 0)) 
            or (self.combined_speed + tp.speed <= ALGO_CAPACITY)
        ):
            return True
        return False

    def biggest_speed(self): 
        return functools.reduce(
            lambda accum, tp: accum if accum > tp.speed else tp.speed,
            self.partitions(), 0
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

    def pretty_print(self): 
        for topic in self.assignment.values(): 
            logger.debug(f"- {topic.topic_name}: {topic.partitions}")
        logger.debug(f"- {self.combined_speed}")

    def __sub__(self, other):
        if self.consumer_id != other.consumer_id: 
            raise Exception()
        return DataConsumer(self.consumer_id, assignment=self.assignment-other.assignment)

    def to_json(self): 
        return [topic.to_json() for topic in self.assignment.values()]


class ConsumerList(list):
    """Mapping to keep track a list of consumers.

    This class maintains the list's functionalities, and extends it's use to the
    specific use case of the list of consumers. 
    """
    

    def __init__(self, clist: Optional[List[DataConsumer]] = None): 
        if clist == None: 
            clist = []
        super().__init__()
        self.available_indices = []
        self.map_partition_consumer = {}
        self.last_created_bin = None
        clist = [c for c in clist if c != None]
        clist.sort(key=ConsumerList.idx_sort)
        for c in clist:
            self.add_consumer(c)

    @classmethod
    def from_json(cls, clist): 
        clist = [
            DataConsumer.from_json(i, c_assignment) 
            for i, c_assignment in enumerate(clist)
            if c_assignment != None
        ]
        return cls(clist)

    @staticmethod
    @cmp_to_key
    def idx_sort(c1, c2): 
        return c1.consumer_id - c2.consumer_id

    def create_bin(
        self, 
        idx: Optional[int] = None, 
        consumer: Optional[DataConsumer] = None
    ):
        """Creates a new consumer in the existing list.

        If the idx is provided, the consumer will be created at that index. In
        the case the idx provided exceeds the current size of the list, the
        elements in between the last element and idx are assigned None. 

        idx and consumer are mutually exclusive parameters. idx creates a
        consumer with no assignment, whereas consumer provides the consumer
        which will be referenced in its idx.

        Args: 
            idx: index at which the consumer is to be created
            consumer: consumer ref to put in the idx
        Returns: 
            None

        Raises: 
        """
        if None not in (consumer, idx):
            raise ParameterException()
        if idx == None:
            idx = (consumer.consumer_id if consumer != None else None)

        if idx == None: 
            # make space consumer
            lowest_idx = (self.available_indices[0]
                if len(self.available_indices)
                else len(self) 
            )
            self.add_consumer(DataConsumer(lowest_idx))
            return lowest_idx
        else:
            if idx < 0: 
                raise InvalidRange("Index has to be >= 0")
            consumer = consumer if consumer != None else DataConsumer(idx)
            self.add_consumer(consumer)
            return idx

    def make_space_consumer(self, idx): 
        """Increase the ConsumerList size to include idx."""
        last_idx = len(self)-1
        for i in range(max(idx-last_idx, 0)): 
            self.append(None)
            self.available_indices.append(last_idx+(i+1))
    
    def add_consumer(self, consumer: DataConsumer): 
        """Add consumer to the calling consumer list"""
        idx = consumer.consumer_id
        self.make_space_consumer(idx)
        pos = bisect.bisect_left(self.available_indices, idx)
        if ((pos == len(self.available_indices))
            or (self.available_indices[pos] != idx) 
        ):
            raise ConsumerAlreadyExists()
        self.available_indices.pop(pos)
        if self[idx] != None: 
            raise ConsumerAlreadyExists()
        self[idx] = consumer
        for p in consumer.partitions(): 
            self.map_partition_consumer[p] = consumer
        self.last_created_bin = self[idx] 

    def remove_bin(self, idx: int): 
        """Remove a bin from the calling consumer list."""
        pass

    def assign_partition_consumer(self, idx, tp): 
        """Assign partition to the consumer in index idx."""
        if((idx >= len(self)) or (idx < -len(self))): 
            raise Exception()
        if(self[idx] == None): 
            raise Exception()
        self[idx].add_partition(tp)
        self.map_partition_consumer[tp] = self[idx]

    def get_idx(self, idx: int): 
        """Return the consumer in index idx or None if it doesn't exist."""
        if (-1*len(self) <= idx < len(self)): 
            return self[idx]
        return None

    def get_consumer(self, tp: TopicPartitionConsumer):
        """Return the consumer that is assigned the partition tp."""
        return self.map_partition_consumer.get(tp)

    def __sub__(self, other): 
        """Compute the difference between two consumer list instances.

        The difference returns a delta of the actions that have to be performed
        by each consumer to reach the intended state. 
        """
        if not isinstance(other, ConsumerList): 
            raise Exception()
        gm = GroupManagement()
        for i, (final, current) in enumerate(itertools.zip_longest(self, other)):
            if (final, current) == (None, None): 
                continue
            final = final if final != None else DataConsumer(i)
            current = current if current != None else DataConsumer(i)
            gm.consumer_difference(current, final)
        self.generate_active_consumers()
        return gm

    def generate_active_consumers(self): 
        active_consumers = set(c for c in self if c != None) 
        if len(self.available_indices):
            fail_safe = set([DataConsumer(self.available_indices[0])])
        else: 
            fail_safe = set([DataConsumer(len(self))])
        self.active_consumers = active_consumers | fail_safe

    def partitions(self):
        """Return all partitions assigned to the consumer list."""
        all_partitions = PartitionSet()
        for c in self:
            if c == None:
                continue
            all_partitions = all_partitions | c.partitions()
        return all_partitions

    def pretty_print(self): 
        for consumer in self:
            if consumer != None:
                logger.debug(f"{ {consumer.consumer_id+1} }")
                consumer.pretty_print()

    def to_json(self): 
        return [
            consumer.to_json() if consumer != None else None
            for consumer in self
        ]


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

    def empty(self):
        return (self.start, self.stop) == (None, None)


class GroupManagement:


    def __init__(self): 
        self.batch = ConsumerMessageBatch()
        self.map_partition_actions = {}
        self.active_consumers = set()

    def add_action(self, action: Command): 
        p_actions = self.map_partition_actions.get(action.partition)
        if p_actions == None:
            p_actions = PartitionCommands(action.partition)
            self.map_partition_actions[action.partition] = p_actions

        if action.__class__ == StopCommand:
            if p_actions.start != None:
                self.batch.remove_action(p_actions.start)
            self.batch.add_action(action)
        elif action.__class__ == StartCommand: 
            if p_actions.stop == None:
                self.batch.add_action(action)
        p_actions.add_action(action)

    def remove_action(self, event_type, partition: TopicPartitionConsumer):
        p_actions = self.map_partition_actions.get(partition)
        if p_actions == None:
            raise Exception()

        if event_type == StopEvent:
            event_type = StopCommand
            consumer = p_actions.stop.consumer
            if p_actions.start != None: 
                self.batch.add_action(p_actions.start)
        elif event_type == StartEvent:
            event_type = StartCommand
            consumer = p_actions.start.consumer
        p_actions.remove_action(event_type(consumer, partition))

        if p_actions.empty(): 
            self.map_partition_actions.pop(partition)

    def consumer_difference(self, current, final): 
        start = final - current
        stop = current - final
        for partition in start.partitions():
            action = StartCommand(final, partition)
            self.add_action(action)
        for partition in stop.partitions():
            action = StopCommand(final, partition)
            self.add_action(action)

    def prepare_batch(self, event_type: Type[Event], record):
        for topic_partitions in record:
            for partition in topic_partitions["partitions"]:
                self.remove_action(
                    event_type,
                    TopicPartitionConsumer(
                        topic_partitions["topic_name"], partition
                    ),
                )

    def empty(self): 
        return len(self.map_partition_actions) == 0



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

    def remove_action(self, action: Command):
        cmsg = self.get(action.consumer)
        if cmsg == None:
            raise Exception()
        cmsg.remove_action(action)



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
            topic.add_partition(action.partition)

    def remove_action(self, action: Command): 
        if action.__class__ == StartCommand: 
            topic = self.start.get(action.partition.topic)
            if topic == None:
                return 
            topic.remove_partition(action.partition)
            if len(topic.partitions) == 0: 
                self.start.pop(action.partition.topic)
        elif action.__class__ == StopCommand: 
            topic = self.stop.get(action.partition.topic)
            if topic == None:
                return 
            topic.remove_partition(action.partition)
            if len(topic.partitions) == 0: 
                self.stop.pop(action.partition.topic)

    def to_record_list(self): 
        l = []
        if self.start != TopicDictConsumer():
            l.append(self.start.to_record())
        if self.stop != TopicDictConsumer():
            l.append(self.stop.to_record())
        return l

