import json
import yaml

from kubernetes.client import (
    Configuration, AppsV1Api, ApiClient
)

with open('test-deployment.yml', 'r') as f:
    body = yaml.safe_load(f)

configuration = Configuration()
with open('token', 'r') as f_token, open('cluster-ip', 'r') as f_ip:
    token = f_token.read().replace('\n', '')
    cluster_ip = f_ip.read().replace('\n', '')
configuration.api_key["authorization"] = token 
configuration.api_key_prefix["authorization"] = "Bearer"
configuration.host = f"https://{cluster_ip}"
configuration.ssl_ca_cert = 'cluster.ca'

with ApiClient(configuration) as api_client:
    c = AppsV1Api(api_client)
    deployments = c.list_namespaced_deployment("data-engineering-dev")
    for dep in deployments.items:
        print(dep.metadata.name)
    # c.delete_namespaced_deployment("busybox", "data-engineering-dev")
