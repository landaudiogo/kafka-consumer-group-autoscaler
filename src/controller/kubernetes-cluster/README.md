# Overview

This directory is intended to set up the cluster to allow the controller to do
its job properly. 

Each of the resources contained within the yaml files, are what grant the
controller permission to the cluster.

The service account is what the controller uses to authenticate itself. To use
the service account within the python process, the token of the service account
is required. 

> Stack overflow discussion as to how to access the cluster.
> https://stackoverflow.com/questions/48151388/kubernetes-python-client-authentication-issue

Additionally to creating the service account, A Role has to be created and then
binded to the service account to grant it permissions to create and check
resources within the cluster. 

# Create the data-engineering namespace

```sh
kubectl apply -f data-engineering-namespace.yml
```

# Create the service account and retrieve the token

```sh
kubectl apply -f service-account 
kubectl describe serviceaccount -n data-engineering-dev controller-service-account
kubectl describe secrets -n data-engineering-dev <token-name>
```

# Create the Role

```sh
kubectl apply -f controller-role.yml
```

# Create the RoleBinding

```sh
kubectl apply -f controller-role-binding.yml
```

# Connecting a python process to the cluster-ip

The code presented in test.py demonstrates how the cluster can be accessed using
a python process, referencing the different files that have to be created with
the data obtained in the previous steps. 

The files are: 
1. cluster.ca: cluster certificate authority that can be obtained through the
   following steps. Access console.cloud.google.com -> Kubernetes Engine in the
   Navigation Menu -> select the cluster through its name. On this page you will
   find the cluster certificate authority.
2. cluster-ip: cluster ip which is obtained in the same page as the previous
   file.
3. token: obtained using the last command of the second header "Create the service account and retrieve the token"
