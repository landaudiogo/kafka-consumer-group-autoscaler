# Persisting Data on a Pod

The intention is to have a pod start and write to a volume, and in case the pod
fails, then the data it wrote to the volume persists when the pod is restarted,
meaning it can be accessed again.

## Volumes

> Kubernetes document on the need for volumes
> https://kubernetes.io/docs/concepts/storage/volumes/

One problem containers presents is the fact that when it crashes it loses its
files. Also implying that when it starts up again, it starts with a clean slate.

A second problem is sharing files between containers that belong to the same
pod. This is where volumes come in.

## Background

Docker provides volumes which represent a directory on disk or in another
container.

Kubernetes supports many types of volumes. A Pod can use any number of volume 
types simultaneously. There is the possibility to use ephemeral volume types
which have the same lifetime as the pod, but persistent volumes exist beyong the
liftime of a pod. 

At its core, a volume is a directory, possibly with some data in it, which is
accessible to the containers in a pod. How that directory comes to be, the
medium that backs it, and the contents of it are determined by the particular
volume type used.

## Types of Volumes

configMap - provides a way to inject ocnfiguration data into pods. The data
stored in a configMap can then be consumed by containerized applications runnins
in a pod. Check examples in the above link to see how this is used.


**downwardAPI** - makes downward API data available to applications. It mounts a
directory and writes the requested data in plain text files. 
> https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/

**emptyDir** - This type of volume is first created when a Pod is assigned to a
node, and exists for as long as the pod is running in the node. emptyDir starts
initially as empty, and all the containers in the Pod can access its contents.

**gcePersistentDisk** - mounts a Google Compute Engine persistent disk into a Pod.
Unlike emptyDir, the contents of a PD are preserved and the volume is merely
unmounted when a pod is removed. The PD can also be pre-populated with data, and
can be shared between pods.

To create a GCE persistent disk (PD)
```bash
gcloud compute disks create --size=500GB --zone=us-central1-a my-data-disk
```

**GCE CSI migration** - 


## Persistent Volume (PV) and Persisten Volume Claim (PVC)

> https://kubernetes.io/docs/concepts/storage/persistent-volumes/

A PersistentVolume (PV) is a piece of storage in the cluster that has been
provisioned by an administrator or dynamically provisioned using Storage
Classes. It is a resource in the cluster just like a node is a cluster resource.
PVs are volume plugins like Volumes, but have a lifecycle independent of any
individual Pod that uses the PV. 

A PersistentVolumeClaim (PVC) is a request for storage by a user. It is similar
to a Pod. Pods consume resources from a node, and PVCs consume PV resources.
Pods can request specific levels of resources (CPU and Memory). PVCs can request
specific size and access modes (e.g. they can be mounted RWO, ROM, RWM, ...).

### Lifecycle of a PV and PVC

PVs are resources in the cluster. PVCs are requests for those resources and also
act as claim checks to the resource. 

**Provisioning** - PVs can be provisioned either statically or dynamically

1. Static - a cluster administrator creates the PV, which contain the details of
   the real storage, which is available for use by cluster users.

2. Dynamic - When none of the static PVs the administrator created match a
   user's PVC, the cluster may try to dynamically provision a volume specially
   for the PVC. This provisioning is based on StorageClasses: the PVC must
   request a storage class and the administrator must have created and
   configured that class for dynamic provisioning to occur. Claims that request
   the class `""` effectively disable dynamic provisioning for themselves.

**Binding** - A user creates, or in the case of dynamic provisioning, has already
created, a PersistentVolumeClaim with a specific amount of storage requested and
with certain access modes. A control loop in the master watches for new PVCs,
finds a matching PV (if possible), and binds them together. A PVC to PV binding
is a one-to-one mapping, using a ClaimRef which is a bi-directional binding
between the PersistentVolume and the PersistenVolumeClaim.

**Using** - Pods use claims as volumes. The cluster inspects the claim to find
the bound volume and mounts that volume fora  Pod. 


## Storage Class

## Tutorial 

1. Follow the gce tutorial to allow the CSI driver in the cluster: 
> https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/gce-pd-csi-driver
2. Create the storage-class
3. Create a Persistent volume claim which uses the storage class created to bind
   to a volume dynamically
4. Create a pod that uses this PVC. 

