import importlib
import time
import json
import copy
import yaml
import fastavro

from io import BytesIO
from typing import Tuple, List
from confluent_kafka import Consumer, TopicPartition, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions
from kubernetes.client import (
    Configuration, AppsV1Api, ApiClient, CoreV1Api
)

from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY, ADMIN_CONFIG, CONTROLLER_PRODUCER_CONFIG
)
from dstructures import (
    TopicPartitionConsumer, ConsumerList, DataConsumer, GroupManagement,
    StopEvent, StartEvent, ConsumerMessageBatch
)
from state_machine import (
    StateMachine, StateSentinel, StateReassignAlgorithm, StateGroupManagement,
    State
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
        self.state_machine.set_initial("s1")

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
        s1 = StateSentinel(self)
        s2 = StateReassignAlgorithm(self, approximation_algorithm="mwf")
        s3 = StateGroupManagement(self)
        s4 = State(self)
        states = [
            ("s1", s1), 
            ("s2", s2), 
            ("s3", s3),
            ("s4", s4),
        ]
        transitions = [
            ("s1", "s2", s1.time_up),
            ("s1", "s2", s1.full_bin),
            ("s1", "s2", s1.any_unassigned),
            ("s2", "s3", s2.finished_approximation_algorithm),
            ("s3", "s1", s3.group_reached_state),
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

    def create_partitions(self, total_partitions): 
        future = self.kafka_cluster_admin.create_partitions([
            NewPartitions("data-engineering-controller", total_partitions)
        ]).get("data-engineering-controller")
        if future.result() == None: 
            print(f"data-engineering-controller has now {total_partitions} partitions")

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

    def create_consumers(self, consumers: List[DataConsumer]): 
        if len(consumers) == 0:
            return 
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

    def delete_consumers(self, consumers: List[DataConsumer]):
        if not len(consumers):
            return 
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
        self.send_batch(delta.batch)
        delta.batch = ConsumerMessageBatch()
        while not delta.empty():
            msg = self.controller_consumer.poll(timeout=1.0)
            if msg == None: 
                continue
            record = self.value_deserializer(msg)
            print(dict(msg.headers())["event_type"].decode())
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
        self.persist_consumer_state()

    def persist_consumer_state(self): 
        with open("/usr/src/data/consumer_group_state.json", "w") as f:
            json.dump(self.next_assignment.to_json(), f)
        self.consumer_list = self.next_assignment

    def load_consumer_state(self): 
        try: 
            with open("/usr/src/data/consumer_group_state.json", "r") as f: 
                clist = json.load(f)
        except FileNotFoundError as e:
            clist = []
        current = ConsumerList()
        current.from_json(clist)
        self.consumer_list = current
        self.consumer_list.pretty_print()

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
