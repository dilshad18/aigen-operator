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
| ✅ GPU nodes present | Scale **GPU Deployment** up → Scale CPU deployment down |
| ❌ No GPU nodes | Scale **CPU Deployment** up → Scale GPU deployment down |

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


