import importlib
import json
import time
import fastavro

from io import BytesIO
from queue import Queue
from typing import List, Dict

from config import IGNORE_EVENTS, BATCH_BYTES, METADATA_CONF
from confluent_kafka import Consumer, TopicPartition, KafkaError, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from utils import (
    eprint, Row, RowList, BatchList, Batch
)



class DEConsumer(Consumer):
    """ Class that wraps the Kafka Consumer Client to add more functionality to
    the HUUB consuming method from the Kafka topic

    Attributes
    ==========
    :topic: - is a mapping (map<string, DETopic>) where the string is the
    topic name and DETopic is an instance of the data engineering topic
    class. 
  
    :change_state_queue: - queue that has all the messages still to be
    processed to change the consumer state

    :current_assignment: - Set of DETopicPartition which has that
    currently assigned topic partitions.

    """

    def __init__(self, conf, assign_internal=[]): 
        """
        Attributes
        ==========

        :topic_partition: - TopicPartition from which to read the controller data from.
        This is the data that does not follow through to bigquery, and as such
        it is not an element of the class DETopic. This assignment can never be
        revoked.

        :conf: - consumer configurations

        """

        super().__init__(conf)
        self.change_state_queue = Queue()
        self.current_assignment = DETopicDict()
        self.row_list = RowList()

    def consume(self, **kwargs) -> RowList:
        records = super().consume(**kwargs)
        rows = RowList()
        for msg in records:
            if msg.topic() == "data-engineering-controller":
                pass
            else:
                row = self.current_assignment[msg.topic()].deserialize(msg)
                if row != None:
                    rows.append(row)
        if rows: 
            self.row_list.extend(rows)
            return
        time.sleep(0.01)
        
    def process_state_queue(self): 
        future = self.current_assignment.copy()
        if not self.change_state_queue.empty():
            for _ in range(self.change_state_queue.qsize()):
                cse = self.change_state_queue.get()
                if cse.event_type == "StartConsumingCommand":
                    future = future | cse
                elif cse.event_type == "StopConsumingCommand":
                    future = future - cse

            inc_assign_dict = future - self.current_assignment
            print("assign", inc_assign_dict)
            inc_assign = [
                TopicPartition(topic=tp.topic, partition=tp.partition)
                for key, value in inc_assign_dict.items()
                    for tp in value.partitions
            ]
            if inc_assign:
                self.incremental_assign(inc_assign)

            inc_unassign_dict = self.current_assignment - future
            print("unassign", inc_unassign_dict)
            inc_unassign = [
                TopicPartition(topic=tp.topic, partition=tp.partition)
                for key, value in inc_unassign_dict.items()
                    for tp in value.partitions
            ]
            if inc_unassign:
                self.incremental_unassign(inc_unassign)
                inc_unassign = set(
                    DETopicPartition(topic=tp.topic, partition=tp.partition)
                    for tp in inc_unassign
                )
                for i in range(len(self.row_list)-1, -1, -1):
                    row = self.row_list[i]
                    if DETopicPartition(topic=row.topic, partition=row.partition) in inc_unassign:
                        self.row_list.pop(i)

            self.current_assignment = future


    def consume_metadata(self):
        list_obj = self.metadata_consumer.consume()
        for obj in list_obj: 
            self.change_state_queue.put(obj)
        self.process_state_queue()
        self.metadata_consumer.commit()

    def __enter__(self):
        self.metadata_consumer = DEMetadataConsumer(
            {**METADATA_CONF, "value.deserializer": AvroDeserializer()}, 
            [TopicPartition(topic="data-engineering-controller", partition=1)],
            ChangeStateEvent
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.metadata_consumer.close()
        self.close()
        

class DEMetadataConsumer(Consumer):
    

    def __init__(
        self, 
        config: Dict, 
        list_topic_partition: List[TopicPartition],
        obj_from_msg: object
    ):
        self.conf_copy = config.copy()
        self.value_deserializer = self.conf_copy.pop("value.deserializer")
        super().__init__(self.conf_copy)
        self.obj_from_msg = obj_from_msg
        self.assign(list_topic_partition)

    def consume(self):
        """"Data has to be read from the metadata partition until the queue is
        completely empty
        
        KafkaError(PARTITION_EOF) is the parameter to analyze to determine
        whether the partition has no more messages to be read.

        When a partition is read, then it is removed from the current set that
        keeps track of the partitions that have not been fully read.

        dict_topic_obj has as key the topic's name and as value the objects
        created from the messages deserialized from the topic.
        """
        list_obj = []

        earliest, latest = self.get_watermark_offsets(self.assignment()[0])
        committed = self.committed(self.assignment())[0].offset

        while (
            (earliest != latest) and (committed != latest)
            and (self.position(self.assignment())[0].offset != latest)
        ):
            msg = self.poll(timeout=0.01)
            if msg is None: 
                continue
            if msg.error() == None: 
                msg_deserialized = self.value_deserializer(msg)
                obj = self.obj_from_msg(headers=dict(msg.headers()), value=msg_deserialized)
                list_obj.append(obj)

        return list_obj




class AvroDeserializer:


    def __init__(self): 
        self.__writer_schemas = {}

    def __call__(self, msg) -> dict:
        serializer = dict(msg.headers())["serializer"].decode()

        writer_schema = self.__writer_schemas.get(serializer)
        if writer_schema == None:
            class_path = serializer.split('.')
            module_path, class_name = '.'.join(class_path[:-1]), class_path[-1]
            module = importlib.import_module(module_path)
            writer_schema = getattr(module, class_name)
            parsed_schema = fastavro.parse_schema(writer_schema)
            self.__writer_schemas[serializer] = parsed_schema 

        with BytesIO(msg.value()) as stream:
            return fastavro.schemaless_reader(stream, writer_schema)

            

class DETopic:

    def __init__(self, **kwargs):
        """

        Attributes
        ==========
        :topic: - Has 2 keys which identify the location of the HUUB
        topic class responsible for serializing and deserializing messages
        coming from this topic. 
          "module_path" - relative or absolute path of the module where the
          class is defined.
          "class_name" - class name within the module.

        :bq_table: - table strign where the table is to be inserted
        """
        self.kwargs = kwargs
        module = importlib.import_module(kwargs["topic_class"]["module_path"])
        self.topic = getattr(module, kwargs["topic_class"]["class_name"])()
        self.table = kwargs["bq_table"]
        self.partitions = set(
            DETopicPartition(topic=kwargs["topic_name"], partition=partition)
            for partition in kwargs["partitions"]
        )
        self.ignore_events = kwargs["ignore_events"]


    def copy(self): 
        return DETopic(**{
            "topic_name": self.kwargs["topic_name"],
            "topic_class": self.kwargs["topic_class"],
            "partitions": [p.partition for p in self.partitions],
            "bq_table": self.table,
            "ignore_events": self.ignore_events
        })

    def deserialize_msg_value(self, msg): 
        item_type_name = dict(msg.headers()).get('item_type_name').decode('utf-8')
        with BytesIO(msg.value()) as stream:
            return self.topic.deserialize(stream, item_type_name)._asdict()

    def deserialize(self, msg) -> Row:
        event_type = (
            dict(msg.headers())['item_type_name'].decode().split('.')[-1]
        )
        if event_type in self.ignore_events:
            return None
        with BytesIO(msg.value()) as stream:
            try: 
                return Row(
                    msg.topic(), msg.partition(), msg.offset(),
                    {
                        'event_type': event_type,
                        'event_json': json.dumps(
                            self.deserialize_msg_value(msg)
                        ),
                        'stream_timestamp': (
                            time.time_ns()
                        ), 
                        'stream_timestamp_hour': time.strftime(
                            "%Y-%m-%d %H:00:00", 
                            time.gmtime()
                        ),
                        'stream_timestamp_date': time.strftime(
                            "%Y-%m-%d", 
                            time.gmtime()
                        ), 
                    }, 
                    self.table 
                )
            except Exception as e:
                eprint(e)
                eprint(f"=== Failed to deserialize message from "
                       f"topic => {msg.topic()}, partition => {msg.partition()} "
                       f"offset => {msg.offset()} ===")
                raise e

    def __repr__(self): 
        plist = [p.partition for p in self.partitions]
        return f"{plist}"



class DETopicPartition(TopicPartition): 
    """Class that wraps the confluent Kafka TopicPartition Class which allows
    any instance of this class to part of a Set.

    This is useful for operations which involve removing, adding and comparing
    sets.

    """


    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (
            True 
            if (self.topic, self.partition) == (other.topic, other.partition) 
            else False
        )



class DETopicDict(dict):


    def __init__(self, **kwargs): 
        super().__init__(**kwargs)

    def __or__(self, other):
        ret = DETopicDict()
        for key, value in self.items(): 
            ret[key] = value.copy()
        for key, value in other.items():
            if ret.get(key) != None:
                ret[key].partitions = ret[key].partitions | value.partitions
            else: 
                ret[key] = value.copy()
        return ret

    def __sub__(self, other):
        ret = DETopicDict()
        for key, value in self.items():
            ret[key] = value.copy()
            if other.get(key) != None:
                ret[key].partitions = ret[key].partitions - other[key].partitions

        return ret

    def copy(self): 
        ret = DETopicDict()
        for key, value in self.items(): 
            ret[key] = value.copy()
        return ret


class ChangeStateEvent(DETopicDict):


    def __init__(self, headers={}, value=[]): 
        self.event_type = headers['event_type'].decode()
        super().__init__(**{
            tpartitions["topic_name"]: DETopic(**tpartitions)
            for tpartitions in value
        })


class SerializationError(Exception): 
    pass
