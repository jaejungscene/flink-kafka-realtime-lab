# Kubernetes Guide

This lab provides Kubernetes manifests for teams that want to move beyond Docker Compose.

## Prerequisites

- A Kubernetes cluster
- Strimzi Kafka Operator installed with KRaft and KafkaNodePool support
- Flink Kubernetes Operator installed
- Container images pushed to a registry reachable by the cluster:
  - `realtime-lab-flink-job:2.1.2`
  - `realtime-lab-api:latest`
  - `realtime-lab-generator:latest`

## Render Manifests

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/prod-like
```

## Apply Order

```bash
kubectl apply -k k8s/overlays/dev
kubectl -n realtime-lab get kafka,kafkatopic,flinkdeployment,pod
```

The Strimzi and Flink custom resources require their operators and CRDs to exist before applying the manifests.

## Dev Overlay

The dev overlay uses:

- 1 Kafka broker
- 1 dual-role KafkaNodePool node
- ephemeral Kafka storage
- 1 Flink TaskManager
- stateless Flink upgrade mode
- short generator run

## Prod-Like Overlay

The prod-like overlay demonstrates:

- 3 Kafka brokers
- 3 dual-role KafkaNodePool nodes
- persistent Kafka storage
- topic replication factor 3
- 2 Flink TaskManagers
- savepoint upgrade mode
- explicit checkpoint path placeholder

Before real production use, replace placeholder image names, configure TLS/auth, choose durable checkpoint storage, and wire metrics into the company monitoring stack.
