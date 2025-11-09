# aigen-operator

The **AIGen Operator** is a Kubernetes controller that automatically manages the scaling of two deployments based on the presence of GPU nodes in the cluster. It ensures that GPU workloads run only when GPU nodes are available, and falls back to CPU deployments when no GPU nodes exist.

## Overview

The operator watches:
- Kubernetes **Nodes**
- The **AIGen** Custom Resource (CR)

Based on detected node capacity:
- If **GPU nodes are available**:  
  → Scale **GPU Deployment** to match number of GPU nodes  
  → Scale CPU deployment down to zero  

- If **no GPU nodes are available**:  
  → Scale **CPU Deployment** to match number of CPU nodes  
  → Scale GPU deployment down to zero  

The operator continuously reconciles this every 60 seconds.

---

## Custom Resource Definition (CRD)

The CRD group is `infra.whiz.ai` and resource kind is `AIGen`.  
Example CR:

```yaml
apiVersion: infra.whiz.ai/v1
kind: AIGen
metadata:
  name: aigen
  namespace: whiz-operator
spec:
  targetNamespace: whiz-ai-gen
  whizCpuDeployment: whiz-ai-gen-cpu
  whizGpuDeployment: whiz-ai-gen-gpu
