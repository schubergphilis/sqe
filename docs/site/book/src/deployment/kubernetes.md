# Kubernetes & Helm

SQE includes a Helm chart for production Kubernetes deployment.

## Architecture on K8s

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Coordinator Deployment"
            C1["sqe-server<br/>--mode coordinator"]
        end

        subgraph "Worker Deployment (optional)"
            W1["sqe-server<br/>--mode worker"]
            W2["sqe-server<br/>--mode worker"]
        end

        SVC["Service: sqe-coordinator<br/>ClusterIP"]
        CM["ConfigMap: sqe-config<br/>(sqe.toml)"]
        SEC["Secret: sqe-secrets<br/>(credentials)"]
        SM["ServiceMonitor<br/>(optional)"]

        SVC --> C1
        CM --> C1
        CM --> W1
        CM --> W2
        SEC --> C1
        SEC --> W1
        SEC --> W2
        C1 --> W1
        C1 --> W2
    end

    CLIENT["Clients"] --> SVC
    PROM["Prometheus"] --> SM
```

## Install with Helm

### Single-Node (small environments)

```bash
helm install sqe deploy/helm/sqe/ \
  --set config.auth.keycloak_url=https://keycloak.example.com \
  --set config.catalog.catalog_url=http://polaris:8181/api/catalog \
  --set secrets.SQE_AUTH__CLIENT_SECRET=my-secret \
  --set secrets.SQE_STORAGE__S3_ACCESS_KEY=minioadmin \
  --set secrets.SQE_STORAGE__S3_SECRET_KEY=minioadmin
```

Workers are **disabled by default** — the coordinator runs queries locally.

### Distributed (production)

```bash
helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set worker.replicas=4 \
  --set coordinator.resources.limits.memory=4Gi \
  --set worker.resources.limits.memory=16Gi \
  --set worker.resources.limits.cpu=8 \
  --set config.auth.keycloak_url=https://keycloak.example.com \
  --set config.catalog.catalog_url=http://polaris:8181/api/catalog \
  --set existingSecret=sqe-credentials
```

### Using an Existing Secret

Create the secret separately (e.g., via sealed-secrets or external-secrets-operator):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: sqe-credentials
type: Opaque
stringData:
  SQE_AUTH__CLIENT_SECRET: "my-secret"
  SQE_STORAGE__S3_ACCESS_KEY: "AKIA..."
  SQE_STORAGE__S3_SECRET_KEY: "wJalrXUtnFEMI/K7MDENG..."
```

Then reference it:
```bash
helm install sqe deploy/helm/sqe/ --set existingSecret=sqe-credentials
```

## Values Reference

### Image

```yaml
image:
  repository: sqe
  tag: latest           # Defaults to Chart.appVersion
  pullPolicy: IfNotPresent
imagePullSecrets: []
```

### Coordinator

```yaml
coordinator:
  replicas: 1
  resources:
    requests: { memory: "512Mi", cpu: "500m" }
    limits:   { memory: "2Gi",   cpu: "2" }
  nodeSelector: {}
  tolerations: []
  affinity: {}
  podAnnotations: {}
```

### Workers

```yaml
worker:
  enabled: false         # Enable for distributed execution
  replicas: 2
  resources:
    requests: { memory: "1Gi", cpu: "1" }
    limits:   { memory: "8Gi", cpu: "4" }
  nodeSelector: {}
  tolerations: []
  affinity: {}
  podAnnotations: {}
```

### Service

```yaml
service:
  type: ClusterIP
  flightSqlPort: 50051
  trinoHttpPort: 8080
  metricsPort: 9090
```

### Health Probes

```yaml
healthPort: 9091
livenessProbe:
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Monitoring

```yaml
serviceMonitor:
  enabled: false
  interval: 30s
  labels: {}            # e.g., { release: prometheus }
```

## Operations

### Scaling Workers

```bash
kubectl scale deployment sqe-worker --replicas=8
# or
helm upgrade sqe deploy/helm/sqe/ --set worker.replicas=8
```

### Rolling Update

Config changes trigger automatic rolling restarts (via checksum annotation on the ConfigMap):

```bash
helm upgrade sqe deploy/helm/sqe/ --set config.catalog.metadata_cache_ttl_secs=60
```

### Interactive SQL

```bash
kubectl exec -it deploy/sqe-coordinator -- sqe-cli
```

### Port Forwarding

```bash
# Flight SQL
kubectl port-forward svc/sqe-coordinator 50051:50051

# Trino HTTP (for dashboards)
kubectl port-forward svc/sqe-coordinator 8080:8080

# Metrics
kubectl port-forward svc/sqe-coordinator 9090:9090
```

### Logs

```bash
kubectl logs deploy/sqe-coordinator -f
kubectl logs deploy/sqe-worker -f
```

Logs are structured JSON — pipe to `jq` for readability:
```bash
kubectl logs deploy/sqe-coordinator | jq .
```

## Worker Secret (distributed mode)

In distributed mode the coordinator and every worker share a secret that authenticates worker registration and credential push. The engine refuses to start when `coordinator_url` / `worker_urls` is set with an empty `worker_secret`, so a distributed install without a secret crashloops. The chart renders `coordinator_url` only when `worker.enabled=true`, so a single-node install needs no secret.

Provide the secret one of two ways. The chart injects the value under both `SQE_COORDINATOR__WORKER_SECRET` and `SQE_WORKER__WORKER_SECRET` from one Secret key, so the guards on both pods are satisfied with matching values.

```bash
# Preferred: reference a Secret you manage, naming the key that holds the value.
kubectl create secret generic sqe-worker-secret \
  --from-literal=SQE_WORKER_SECRET="$(openssl rand -hex 32)"

helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set workerSecret.existingSecret=sqe-worker-secret \
  --set workerSecret.key=SQE_WORKER_SECRET

# Dev/test: inline value. The chart creates the Secret for you.
helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set workerSecret.value=dev-only-shared-secret
```

## Availability and Disruption Budgets

The chart ships PodDisruptionBudgets and default pod anti-affinity so a node drain or rolling upgrade cannot take the cluster down at once.

- **Coordinator PDB**: `minAvailable: 1`. The coordinator is single-replica today, so this blocks an unforced eviction of the only coordinator. A node drain that targets it will not proceed until you act (cordon and delete the pod, or scale the deployment to 0 first). The budget protects a single point of failure; it does not provide HA.
- **Worker PDB**: `maxUnavailable: 1`. A drain rolls through one node at a time while the rest keep serving fragments. Rendered only when `worker.enabled`.
- **Anti-affinity**: when a component's `affinity` is empty, the chart applies a preferred `podAntiAffinity` by hostname so replicas spread across nodes. Workers always get it; the coordinator gets it only at `replicas > 1`. Set `affinity` to override the default entirely, or `defaultAntiAffinity: false` to render none.

Toggle the budgets with `podDisruptionBudget.enabled` (default `true`) and tune `podDisruptionBudget.coordinator.minAvailable` / `podDisruptionBudget.worker.maxUnavailable`.

### The coordinator is a single point of failure

The coordinator runs as a single replica. Session state, the worker registry, and in-flight query state are process-local; there is no shared store. A coordinator restart drops every in-flight query and invalidates client sessions, so connected clients must re-authenticate and re-run. A node drain that moves the coordinator pod is a brief outage, not a transparent failover.

Running more than one coordinator replica is not yet safe. Two replicas do not share sessions or the registry, so clients would land on a coordinator that has never seen their session. Keep `coordinator.replicas: 1`. Full coordinator HA with shared session and registry state is a separate design.
