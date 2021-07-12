from kubernetes import (
    config, 
)
from kubernetes.client import (
    ApiClient, 
    AppsV1Api
)
import yaml

yaml_file = 'sms.yaml'


config.load_kube_config('config')
files = ['delivery', 'carrier_account', 'sms', 'shipping_prices', 'ext']

with ApiClient() as api_client:
    for file_name in files:
        with open(f'deployments/{file_name}.yaml') as f: 
            dep = yaml.safe_load(f)
            c = AppsV1Api(api_client)
            resp = c.create_namespaced_deployment(
                body=dep, namespace="default"
            )
            print(f"Deployment created. {resp.metadata.name}")
