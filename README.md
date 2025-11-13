# aigen-operator

The **AIGen Operator** is a Kubernetes controller built with [Kopf](https://kopf.readthedocs.io/) that automatically manages the scaling of two deployments — one for CPU and one for GPU — based on the availability of GPU nodes in the cluster.

It ensures that GPU workloads run only when GPU nodes are present and gracefully falls back to CPU deployments otherwise.

---

## 🧩 Overview

The operator watches:
- Kubernetes **Nodes**
- The **AIGen** Custom Resource (CR)

It reacts dynamically to node and CR changes, and also reconciles periodically every 60 seconds.

| GPU Node Availability | Action Taken |
|------------------------|--------------|
| ✅ GPU nodes | Scale **GPU Deployment** up → Scale CPU deployment down |
| ✅ CPU nodes | Scale **CPU Deployment** up → Scale GPU deployment down |

---

## ⚙️ Custom Resource Definition (CRD)

Group: `infra.whiz.ai`  
Version: `v1`  
Kind: `AIGen`

Example CR:

```yaml
apiVersion: infra.whiz.ai/v1
kind: AIGen
metadata:
  name: aigen
  namespace: whiz-operator
spec:
  targetNamespace: whiz-ai-gen
  cpuDeployment: whiz-ai-gen-cpu
  gpuDeployment: whiz-ai-gen-gpu
  replicas: 2

```
### Status Updates

The operator updates the status field in the AIGen Custom Resource (CR) to reflect the current deployment state.
```bash
NAME    ACTIVE DEPLOYMENT   NAMESPACE   REPLICAS   LAST SYNC                          REASON               AGE
aigen   whiz-ai-gen-cpu     test        1          2025-11-12T09:12:57.813768+00:00   CPU nodes detected   2d14h
```
#### This shows:

ACTIVE DEPLOYMENT: Which deployment (cpu or gpu) is currently active

NAMESPACE: Target namespace where it's running

REPLICAS: Number of active replicas

LAST SYNC: When the last reconciliation happened

REASON: Why the switch occurred (e.g., GPU nodes detected or CPU nodes detected) 