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
    Configuration, AppsV1Api, ApiClient
)

from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY, ADMIN_CONFIG, CONTROLLER_PRODUCER_CONFIG
)
from dstructures import (
    TopicPartitionConsumer, ConsumerList, DataConsumer, GroupManagement
)
from state_machine import (
    StateMachine, StateSentinel, StateReassignAlgorithm, StateGroupManagement,
    State
)
from de_avro import DEControllerSchema



class Controller: 


    def __init__(self):
        self.monitor_consumer = Consumer(MONITOR_CONSUMER_CONFIG)
        _, next_off = self.monitor_consumer.get_watermark_offsets(
            TopicPartition(topic="data-engineering-monitor", partition=0)
        )
        last_off = next_off - 1
        self.monitor_consumer.assign([TopicPartition(
            topic="data-engineering-monitor", partition=0, offset=last_off
        )])
        while True:
            cstate = self.monitor_consumer.assignment()[0].offset
            if cstate == last_off:
                break
            time.sleep(0.01)

        self.controller_consumer = Consumer(CONTROLLER_CONSUMER_CONFIG)
        self.controller_consumer.assign([TopicPartition(
            topic="data-engineering-controller", partition=0
        )])
        self.controller_producer = Producer(CONTROLLER_PRODUCER_CONFIG)
        self.de_controller_metadata = Consumer(CONTROLLER_CONSUMER_CONFIG)
        self.kafka_cluster_admin = AdminClient(ADMIN_CONFIG)


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
            ("s3", "s4", s3.group_reached_state),
        ]
        self.state_machine = StateMachine(
            self, states=states, transitions=transitions
        )
        self.state_machine.set_initial("s1")

        self.unassigned_partitions = []
        self.consumer_list = ConsumerList()
        self.next_assignment = None

        configuration = Configuration()
        with open('kubernetes-cluster/token', 'r') as f_token, \
             open('kubernetes-cluster/cluster-ip', 'r') as f_ip, \
             open('template-deployment.yml', 'r') as f_td:
            token = f_token.read().replace('\n', '')
            cluster_ip = f_ip.read().replace('\n', '')
            self.template_deployment = yaml.safe_load(f_td)
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

    def create_consumers(self, consumers: List[DataConsumer]): 
        if not len(consumers):
            return 
        with ApiClient(self.kube_configuration) as api_client:
            kube_client = AppsV1Api(api_client)
            existing_deployments = [
                dep.metadata.name
                for dep in kube_client.list_namespaced_deployment("data-engineering-dev").items
            ]
            
            if (len(self.next_assignment)+1) > self.get_num_partitions():
                self.create_partitions(len(self.next_assignment) + 1)

            for consumer in consumers:
                deployment_id = f'{self.template_deployment["metadata"]["name"]}-{consumer.consumer_id+1}'
                if deployment_id in existing_deployments:
                    continue
                body = self.change_template_deployment(deployment_id)
                deployments = kube_client.create_namespaced_deployment("data-engineering-dev", body)
                print(f"created consumer with id {deployment_id}")

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
        return body

    def change_consumers_state(self, delta: GroupManagement):
        for consumer, cmessages in delta.batch.items():
            for record in cmessages.to_record_list():
                print("=> ", consumer.consumer_id+1, " <=")
                print(record)
                with BytesIO() as stream:
                    parsed_schema = fastavro.parse_schema(DEControllerSchema)
                    fastavro.schemaless_writer(
                        stream, parsed_schema, record["payload"]
                    )
                    self.controller_producer.produce(
                        "data-engineering-controller",
                        value=stream.getvalue(),
                        headers=record["headers"],
                        partition=consumer.consumer_id+1
                    )
        self.controller_producer.flush()
        while True:
            msg = self.controller_consumer.poll(timeout=1.0)
            if msg == None: 
                continue
            else:
                with BytesIO(msg.value()) as stream:
                    record = fastavro.schemaless_reader(stream, parsed_schema)
                    self.controller_consumer.commit()
                    break

    def run(self): 
        while True:
            self.state_machine.execute()
