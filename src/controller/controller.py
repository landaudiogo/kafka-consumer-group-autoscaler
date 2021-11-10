import time
import json
import copy
import yaml

from typing import Tuple, List
from confluent_kafka import Consumer, TopicPartition
from kubernetes.client import (
    Configuration, AppsV1Api, ApiClient
)

from config import (
    MONITOR_CONSUMER_CONFIG, CONTROLLER_CONSUMER_CONFIG, 
    CONSUMER_CAPACITY
)
from dstructures import TopicPartitionConsumer, ConsumerList, DataConsumer
from state_machine import (
    StateMachine, StateSentinel, StateReassignAlgorithm, StateGroupManagement,
    State
)



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
            topic="data-engineering-monitor", partition=0
        )])


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
        with ApiClient(self.kube_configuration) as api_client:
            c = AppsV1Api(api_client)
            existing_deployments = [
                dep.metadata.name
                for dep in c.list_namespaced_deployment("data-engineering-dev").items
            ]
            print(existing_deployments)
            for consumer in consumers:
                deployment_id = f'{self.template_deployment["metadata"]["name"]}-{consumer.consumer_id+1}'
                if deployment_id in existing_deployments:
                    continue
                body = copy.deepcopy(self.template_deployment)
                body["metadata"]["name"] = deployment_id
                body["metadata"]["labels"]["app"] = deployment_id
                body["spec"]["selector"]["matchLabels"]["app"] = deployment_id
                body["spec"]["template"]["metadata"]["labels"]["app"] = deployment_id
                deployments = c.create_namespaced_deployment("data-engineering-dev", body)
                print(f"created consumer with id {deployment_id}")

    def run(self): 
        while True:
            self.state_machine.execute()
