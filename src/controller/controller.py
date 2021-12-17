import importlib
import time
import json
import copy
import yaml
import fastavro
import re

from io import BytesIO
from typing import Tuple, List, Set
from confluent_kafka import Consumer, TopicPartition, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions
from kubernetes.client import (
    Configuration, AppsV1Api, ApiClient, CoreV1Api
)

from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY, ADMIN_CONFIG, CONTROLLER_PRODUCER_CONFIG, 
    MAX_TIME_STATE_GM
)
from dstructures import (
    TopicPartitionConsumer, ConsumerList, DataConsumer, GroupManagement,
    StopEvent, StartEvent, ConsumerMessageBatch
)
from state_machine import (
    StateMachine, StateSentinel, StateReassignAlgorithm, StateGroupManagement,
    StateInitialize, StateSynchronize
)
from de_avro import DEControllerSchema



class Controller: 


    def __init__(self):
        self.monitor_consumer = Consumer(MONITOR_CONSUMER_CONFIG)
        self.initialize_monitor_consumer()

        self.controller_consumer = Consumer(CONTROLLER_CONSUMER_CONFIG)
        self.controller_consumer.assign([TopicPartition(
            topic="data-engineering-controller", partition=0
        )])
        self.controller_producer = Producer(CONTROLLER_PRODUCER_CONFIG)
        self.de_controller_metadata = Consumer(CONTROLLER_CONSUMER_CONFIG)
        self.kafka_cluster_admin = AdminClient(ADMIN_CONFIG)

        self.state_machine = self.create_controller_state_machine()
        self.state_machine.set_initial("initialize")

        self.unassigned_partitions = []
        self.consumer_list = None
        self.load_consumer_state()
        self.next_assignment = None

        self.template_deployment = None
        self.template_pvc = None
        self.kube_configuration = None
        self.load_kube_data()

        self.value_deserializer = AvroDeserializer() 
        self.value_serializer = AvroSerializer(DEControllerSchema)

        self.test_speeds = None

    def initialize_monitor_consumer(self): 
        earliest, latest = self.monitor_consumer.get_watermark_offsets(
            TopicPartition(topic="data-engineering-monitor", partition=0)
        )

        last_off = latest - 1 if latest != earliest else latest
        self.monitor_consumer.assign([TopicPartition(
            topic="data-engineering-monitor", partition=0, offset=last_off
        )])
        while True:
            cstate = self.monitor_consumer.assignment()[0].offset
            if cstate == last_off:
                break
            time.sleep(0.01)


    def create_controller_state_machine(self): 
        initialize = StateInitialize(self)
        synchronize = StateSynchronize(self)
        sentinel = StateSentinel(self)
        reassign = StateReassignAlgorithm(self, approximation_algorithm="mwf")
        manage = StateGroupManagement(self)
        states = [
            ("initialize", initialize), 
            ("synchronize", synchronize), 
            ("sentinel", sentinel), 
            ("reassign", reassign), 
            ("manage", manage), 
        ]
        transitions = [
            ("initialize", "synchronize", initialize.no_persisted_state),
            ("initialize", "sentinel", initialize.persisted_state),
            ("synchronize", "sentinel", synchronize.synchronized),
            ("sentinel", "reassign", sentinel.time_up),
            ("sentinel", "reassign", sentinel.full_bin),
            ("sentinel", "reassign", sentinel.any_unassigned),
            ("reassign", "manage", reassign.finished_approximation_algorithm),
            ("manage", "sentinel", manage.group_reached_state),
            ("manage", "synchronize", manage.out_of_sync),
        ]
        return StateMachine(
            self, states=states, transitions=transitions
        )

    def load_kube_data(self):
        configuration = Configuration()
        with open('kubernetes-cluster/token', 'r') as f_token, \
             open('kubernetes-cluster/cluster-ip', 'r') as f_ip, \
             open('template-deployment.yml', 'r') as f_td, \
             open('template-pvc.yml', 'r') as f_tpvc:
            token = f_token.read().replace('\n', '')
            cluster_ip = f_ip.read().replace('\n', '')
            self.template_deployment = yaml.safe_load(f_td)
            self.template_pvc = yaml.safe_load(f_tpvc)
        configuration.api_key["authorization"] = token 
        configuration.api_key_prefix["authorization"] = "Bearer"
        configuration.host = f"https://{cluster_ip}"
        configuration.ssl_ca_cert = 'kubernetes-cluster/cluster.ca'
        self.kube_configuration = configuration

    def get_last_monitor_record(self): 
        if self.test_speeds == None: 
            with open("test/monitor_sequence/measurements_25", "r") as f:
                self.test_speeds = json.load(f)
        idx = self.state_machine.states["sentinel"].ITERATION
        if idx >= len(self.test_speeds):
            exit(0)
        return self.test_speeds[idx]
        start_off, next_off = self.monitor_consumer.get_watermark_offsets(
            TopicPartition(topic="data-engineering-monitor", partition=0)
        )
        if start_off == next_off: 
            return None

        last_off = next_off - 1
        self.monitor_consumer.seek(TopicPartition(
            topic="data-engineering-monitor", partition=0, offset=last_off
        ))
        while True:
            msg = self.monitor_consumer.poll(timeout=0)
            if msg != None: 
                if msg.error() == None:
                    return json.loads(msg.value())
            else: 
                time.sleep(0.01)

    def get_num_partitions(self, topic="data-engineering-controller"):
        d = self.de_controller_metadata.list_topics(topic)
        return len(d.topics[topic].partitions)

    def create_partitions(self, total_partitions, topic="data-engineering-controller"): 
        future = self.kafka_cluster_admin.create_partitions([
            NewPartitions(topic, total_partitions)
        ]).get(topic)
        if future.result() == None: 
            print(f"{topic} has now {total_partitions} partitions")

    def change_template_deployment(self, deployment_id): 
        body = copy.deepcopy(self.template_deployment)
        body["metadata"]["name"] = deployment_id
        body["metadata"]["labels"]["app"] = deployment_id
        body["spec"]["selector"]["matchLabels"]["app"] = deployment_id
        body["spec"]["template"]["metadata"]["labels"]["app"] = deployment_id
        body["spec"]["template"]["spec"]["volumes"][1]["persistentVolumeClaim"]["claimName"] = f"{deployment_id}-volume"
        return body

    def change_template_pvc(self, pvc_id): 
        body = copy.deepcopy(self.template_pvc)
        body["metadata"]["name"] = pvc_id
        return body

    def create_consumers(self): 
        with ApiClient(self.kube_configuration) as api_client:
            core_v1_client = CoreV1Api(api_client)
            apps_v1_client = AppsV1Api(api_client)

            existing_deployments = set(
                dep.metadata.name
                for dep in apps_v1_client.list_namespaced_deployment("data-engineering-dev").items
            )
            existing_pvc = set(
                pvc.metadata.name 
                for pvc in core_v1_client.list_namespaced_persistent_volume_claim("data-engineering-dev").items
            )
            
            if self.get_num_partitions() < (len(self.next_assignment) + 10):
                self.create_partitions(len(self.next_assignment) + 10)
            if self.get_num_partitions(topic="data-engineering-query") < (len(self.next_assignment) + 10):
                self.create_partitions(len(self.next_assignment) + 10, topic="data-engineering-query")

            active_deps = set(
                f"de-consumer-{c.consumer_id+1}"
                for c in self.next_assignment.active_consumers
            )
            for dep_id in active_deps:
                pvc_id = f'{dep_id}-volume'
                if dep_id in existing_deployments:
                    continue
                if pvc_id not in existing_pvc:
                    pvc = self.change_template_pvc(pvc_id)
                    core_v1_client.create_namespaced_persistent_volume_claim("data-engineering-dev", pvc)
                dep = self.change_template_deployment(dep_id)
                apps_v1_client.create_namespaced_deployment("data-engineering-dev", dep)
                print(f"created consumer with id {dep_id}")

    def delete_consumers(self):
        with ApiClient(self.kube_configuration) as api_client:
            apps_v1_client = AppsV1Api(api_client)
            existing_deployments = set(
                dep.metadata.name
                for dep in apps_v1_client.list_namespaced_deployment("data-engineering-dev").items
            )
            active_deps = set(
                f"de-consumer-{c.consumer_id+1}"
                for c in self.next_assignment.active_consumers
            )
            consumers_delete = existing_deployments - active_deps
            for consumer in consumers_delete:
                apps_v1_client.delete_namespaced_deployment(consumer, "data-engineering-dev")
                print(f"removed consumer with id {consumer}")

    def wait_deployments_ready(self): 
        clist_set = set(
            f"de-consumer-{c.consumer_id+1}" 
            for c in self.next_assignment
                if c != None
        )
        with ApiClient(self.kube_configuration) as api_client:
            while True:
                apps_v1_client = AppsV1Api(api_client)
                unavailable_deps = set( 
                    dep.metadata.name
                    for dep in apps_v1_client.list_namespaced_deployment("data-engineering-dev").items
                        if dep.status.unavailable_replicas != None
                )
                if clist_set - unavailable_deps == clist_set: 
                    return 
                time.sleep(0.5)

    def change_consumers_state(self, delta: GroupManagement):
        self.clear_state_file()
        self.send_batch(delta.batch)
        delta.batch = ConsumerMessageBatch()
        start = time.time()
        while not delta.empty():
            if time.time() - start > MAX_TIME_STATE_GM: 
                raise Exception()
            msg = self.controller_consumer.poll(timeout=1.0)
            if msg == None: 
                continue
            etype = dict(msg.headers())["event_type"].decode()
            if etype == "StateQueryResponse":
                continue
            record = self.value_deserializer(msg)
            print(etype)
            for topic in record:
                print("-", topic["topic_name"], topic["partitions"])
            event_type = (
                StartEvent
                if dict(msg.headers())["event_type"].decode() == "StartConsumingEvent"
                else StopEvent
            )
            delta.prepare_batch(event_type, record)
            self.send_batch(delta.batch)
            delta.batch = ConsumerMessageBatch()
            self.controller_consumer.commit()

    def persist_consumer_state(self): 
        with open("/usr/src/data/consumer_group_state.json", "w") as f:
            json.dump(self.consumer_list.to_json(), f)

    def clear_state_file(self): 
        with open("/usr/src/data/consumer_group_state.json", "w") as f: 
            json.dump(None, f)


    def load_consumer_state(self): 
        try: 
            with open("/usr/src/data/consumer_group_state.json", "r") as f: 
                clist = json.load(f)
                return clist
        except FileNotFoundError as e:
            return None


    def send_batch(self, batch): 
        for consumer, cmessages in batch.items():
            print("Consumer =>", consumer)
            for record in cmessages.to_record_list():
                if record["payload"] == []:
                    continue
                print(record["headers"]["event_type"])
                for topic in record["payload"]:
                    print("-", topic["topic_name"], topic["partitions"])
                avro_record = self.value_serializer(record["payload"])
                self.controller_producer.produce(
                    "data-engineering-controller",
                    value=avro_record,
                    headers=record["headers"],
                    partition=consumer.consumer_id+1
                )
        self.controller_producer.flush()

    def active_consumers(self):
        with ApiClient(self.kube_configuration) as api_client:
            apps_v1_client = AppsV1Api(api_client)
            deps = [dep.metadata.name
                for dep in apps_v1_client.list_namespaced_deployment(
                    "data-engineering-dev",
                    label_selector="consumerGroup=de-consumer-group"
                ).items
            ]
        return [int(re.search(r"\w+-\w+-(\d+)", cstr).group(1))-1
            for cstr in deps
        ]

    def new_consumer_list(self, active_consumers: List[int]):
        self.consumer_list = ConsumerList()
        for c in active_consumers:
            self.consumer_list.create_bin(c)
        print(self.consumer_list)

    def query_consumers(self, set_consumers: Set[DataConsumer]):
        for c in set_consumers: 
            print(f"sending a message to consumer with id: {c.consumer_id+1}")
            self.controller_producer.produce(
                "data-engineering-query", 
                value="",
                partition=c.consumer_id+1,
                headers={
                    "serializer": "",
                    "event_type": "StateQuery"
                }
            )
        self.controller_producer.flush()

    def wait_queries_response(self, set_consumers):
        while len(set_consumers):
            msg = self.controller_consumer.poll(timeout=1.0)
            if msg == None: 
                continue
            headers = dict(msg.headers())
            if headers["event_type"].decode() != "StateQueryResponse":
                continue
            record = self.value_deserializer(msg)
            idx = int(dict(msg.headers())["consumer_id"])
            for topic in record: 
                for p in topic["partitions"]: 
                    tp = TopicPartitionConsumer(topic["topic_name"], p)
                    self.consumer_list.assign_partition_consumer(idx, tp)
            consumer = self.consumer_list[idx]
            if consumer in set_consumers:
                set_consumers.remove(consumer)
        self.controller_consumer.commit()
        self.persist_consumer_state()

    def run(self): 
        while True:
            self.state_machine.execute()


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


class AvroSerializer:


    def __init__(self, schema):
        self.parsed_schema = fastavro.parse_schema(schema)

    def __call__(self, record):
        with BytesIO() as stream:
            fastavro.schemaless_writer(
                stream, 
                self.parsed_schema, 
                record
            )
            return stream.getvalue()
