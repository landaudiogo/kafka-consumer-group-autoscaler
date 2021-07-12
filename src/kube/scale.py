from kubernetes import client, config
from kubernetes.client import (
    ApiClient, 
    AppsV1Api
)
import yaml

deployment_name = 'delivery-group'


config.load_kube_config('config')

with ApiClient() as api_client:
    c = AppsV1Api(api_client)
    resp = c.read_namespaced_deployment_scale(
        deployment_name, namespace="default"
    )
    body = resp.to_dict()
    body['spec']['replicas'] = 1
    resp = c.patch_namespaced_deployment_scale(
        deployment_name, "default", body
    )
