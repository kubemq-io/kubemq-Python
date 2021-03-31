# KubeMQ Python SDK 

KubeMQ is an enterprise-grade message queue and broker for containers, designed for any workload and architecture running in Kubernetes.
This library is Python implementation of KubeMQ client connection.


## Getting Started

## Install KubeMQ Cluster/Server

Every installation method requires a KubeMQ key.
Please [register](https://account.kubemq.io/login/register) to obtain your KubeMQ key.

### Kubernetes
#### Option 1

Install KubeMQ cluster on any Kubernetes cluster.
 
Step 1:

``` bash
kubectl apply -f https://deploy.kubemq.io/init
```

Step 2:

``` bash
kubectl apply -f https://deploy.kubemq.io/key/{{your key}}
```
#### Option 2

Build and Deploy KubeMQ Cluster with advanced configurations - [Build & Deploy](https://build.kubemq.io/)

#### Port-Forward KubeMQ Grpc Interface

Use kubectl to port-forward kubemq grpc interface 
```
kubectl port-forward svc/kubemq-cluster-grpc 50000:50000 -n kubemq
```

### Docker

Pull and run KubeMQ standalone docker container:
``` bash
docker run -d -p 8080:8080 -p 50000:50000 -p 9090:9090 KEY={{yourkey}} kubemq/kubemq-standalone:latest
```

### Binaries

KubeMQ standalone binaries are available for Edge locations and for local development.

Steps:

1. Download the latest version of KubeMQ standalone from [Releases](https://github.com/kubemq-io/kubemq/releases)
2. Unpack the downloaded archive
3. Run ```kubemq -k {{your key}}``` (A key is needed for the first time only)

## Install Python SDK
### Prerequisites

KubeMQ-SDK-Python works with **Python 3.2** or newer.

### Installing
 
The recommended way to use the SDK for Python in your project is to consume it from pip.

```
pip install kubemq
```

This package uses setuptools for the installation if needed please run:
```
python3 -m pip install --upgrade pip setuptools wheel
```

### Building from source

Once you check out the code from GitHub, you can install the package locally with:

```
$ pip install .
```

You can also install the package with a symlink, 
so that changes to the source files will be immediately available:

```
$ pip install -e .
```

Installation:
$ pip install kubemq

## Learn KubeMQ
Visit our [Extensive KubeMQ Documentation](https://docs.kubemq.io/).


## Examples - Cookbook Recipes
Please visit our cookbook [repository](https://github.com/kubemq-io/python-sdk-cookbook)

## Support
if you encounter any issues, please open an issue here,
In addition, you can reach us for support by:
- [**Email**](mailto://support@kubemq.io)
- [**Slack**](https://kubmq.slack.com)
