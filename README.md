# Regression Lab

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Unlike [causely-oss/tracey-shop](https://github.com/causely-oss/tracey-shop), whose faults are
runtime toggles you flip on and off against a healthy baseline, this repo's faults are **shipped
code and config regressions** — as if a bad PR had merged. There's no flag to flip back; fixing
one means reading the current source and writing a real diff, the same way you'd fix a production
incident.

## Overview

The platform runs **36 Go microservices** (built with `net/http`, Prometheus client,
sarama for Kafka, go-redis, and pgx for PostgreSQL) with complex service-to-service
communication patterns, Kafka-based async pipelines, Redis caching, and a deep
synchronous call chain — making root cause analysis a realistic challenge.

**Eval prompt:** *"What is causing checkout failures?"*

---

## Architecture

### Service Map (36 services)

```
                                        ┌──────────────────────────────┐
                                        │        load generator        │
                                        │  8 workloads · ~154 rps      │
                                        └──────────────┬───────────────┘
                                                       │
              ┌──────────┬──────────┬──────────┬───────┼───────┬──────────┬──────────┐
              │          │          │          │       │       │          │          │
              v          v          v          v       v       v          v          v
          /checkout   /search   /catalog   /orders  /auth  /cart     /profile   /ingest
              │          │          │          │       │       │          │          │
              v          v          v          v       v       v          v          v
        api-gateway  search-svc  api-gw    api-gw   api-gw  api-gw   profile-svc ingest-svc
           :8081       :8088      :8081     :8081    :8081   :8081      :8090       :8093
              │          │          │          │       │       │          │          │(Kafka)
              v          │          v          v       v       v     ┌────┤          v
          checkout       │    catalog-svc  orders-svc auth  cart    │    │    processing-svc
           :8082         │       :8102       :8085   :8084  :8101   │    │        :8094
              │          │     ┌──┼──┐      ┌─┼──┐    │    ┌─┼──┐  │    │          │(Kafka)
     ┌────┬──┼──┬────┐   │     │  │  │      │ │  │    │    │ │  │  │    v          v
     │    │  │  │    │   │     │  │  │      │ │  │    v    │ │  │  │ loyalty    recommendation
     v    v  v  v    v   │     │  │  │      │ │  │ session │ │  │  │  :8104       :8095
  pricing bill fraud pay │     │  │  │      │ │  │  :8106  │ │  │  │    │      ┌──┼──┐
  :8097  :8091 :8111 :8083     │  │  │      │ │  │         │ │  │  v    v      │  │  │
     │     │    │      │   │     v  v  v      v v  v         v v  v  cache  user  │  v  v
     v     │    v      v   v   review media cache inv ship notif  catalog  :8109 :8099 delivery analytics
  discount │ analytics DB  ranking :8103 :8114 :8109 :8086 :8087 :8100  :8102         :8096   :8107
   :8098   │   :8107       :8089   │                   │     │     │                    │       │
     │     │     │           │     v                   v     v     v                    v       v
     v     │     v           v   recommendation     warehouse email notification    warehouse reporting
  loyalty  │  reporting   profile   :8095            :8113  :8108  :8100            :8113     :8110
   :8104   │    :8110      :8090      │                                                        │
     │     │                 │        v                                                        v
     v     v                 v     analytics → reporting → cache                            cache
  user  payment-adapter   user/media/loyalty/cache                                          :8109
  :8099    :8092            :8099/:8114/:8104/:8109
     │        │
     v        v
  cache   external-payment-api
  :8109      :8115
```

> Every service now makes 2–5 downstream calls (except true leaves: email-service, tax-service,
> external-payment-api). Fan-out calls are fire-and-forget via goroutines so they don't block
> the primary request path.

### Application Flows

| Flow | Path | Type |
|------|------|------|
| **Auth** | frontend → api-gateway → auth-service → session-service (Redis) + user-service → cache-service | Sync HTTP |
| **Checkout** | checkout → pricing-service → discount-service → loyalty-service → cache-service; pricing also → tax-service, cache-service | Sync HTTP |
| **Payment** | checkout → payments-api → payments-db (PostgreSQL); checkout → fraud-detection → analytics-service | Sync HTTP |
| **Billing** | checkout → billing-service → payment-adapter → external-payment-api; billing also → tax-service, fraud-detection, notification-service | Sync HTTP |
| **Order Pipeline** | checkout → Kafka(regression-lab-orders) → orders-service → notification-service, shipping-service; orders → Kafka(regression-lab-inventory-updates) → inventory-service → notification-service; inventory → Kafka(regression-lab-shipping-events) → shipping-service → email-service, analytics-service | Kafka async + Sync HTTP |
| **Search** | frontend → search-service → ranking-service → analytics-service, recommendation-service; ranking → profile-service → loyalty-service, cache-service; profile → user-service → cache-service, media-service → cache-service | Sync HTTP |
| **Deep Chain** | frontend → api-gateway → catalog-service → review-service → user-service, media-service; review → recommendation-service → user-service, cache-service; recommendation → analytics-service → cache-service; analytics → reporting-service → cache-service; catalog also → pricing-service, inventory-service | Sync HTTP |
| **Cart** | frontend → api-gateway → cart-service → catalog-service; cart also → pricing-service, session-service | Sync HTTP |
| **Streaming** | frontend → ingest-service → Kafka(regression-lab-ingest-data) → processing-service → Kafka(regression-lab-recommendations) → recommendation-service → delivery-service → warehouse-service, email-service, notification-service | Kafka async + Sync HTTP |
| **Orders UI** | frontend → api-gateway → orders-service → inventory-service → warehouse-service → Kafka(regression-lab-notifications) → notification-service → email-service | Sync HTTP + Kafka |

### Kafka Topics

| Topic | Producers | Consumers |
|-------|-----------|-----------|
| `regression-lab-orders` | checkout | orders-service |
| `regression-lab-inventory-updates` | orders-service | inventory-service |
| `regression-lab-shipping-events` | inventory-service | shipping-service |
| `regression-lab-notifications` | warehouse-service | notification-service |
| `regression-lab-audit-events` | auth-service, billing-service | audit-service |
| `regression-lab-analytics-events` | search-service, review-service | analytics-service |
| `regression-lab-ingest-data` | ingest-service | processing-service |
| `regression-lab-recommendations` | processing-service | recommendation-service |

### All Services (36)

| Service | Port | Dependencies |
|---------|------|--------------|
| frontend | 8080 | api-gateway, search-service, profile-service, ingest-service |
| api-gateway | 8081 | checkout, auth-service, cart-service, catalog-service, orders-service, profile-service |
| checkout | 8082 | payments-api, pricing-service, billing-service, fraud-detection, Redis, Kafka |
| payments-api | 8083 | payments-db (PostgreSQL) |
| auth-service | 8084 | session-service, user-service, Kafka |
| orders-service | 8085 | inventory-service, notification-service, shipping-service, Kafka |
| inventory-service | 8086 | warehouse-service, notification-service, Redis, Kafka |
| shipping-service | 8087 | warehouse-service, notification-service, email-service, analytics-service, Kafka |
| search-service | 8088 | ranking-service, cache-service, analytics-service, Kafka |
| ranking-service | 8089 | profile-service, analytics-service, recommendation-service, Redis |
| profile-service | 8090 | user-service, media-service, loyalty-service, cache-service |
| billing-service | 8091 | payment-adapter, tax-service, fraud-detection, notification-service, Kafka |
| payment-adapter | 8092 | external-payment-api, notification-service |
| ingest-service | 8093 | Kafka |
| processing-service | 8094 | Kafka |
| recommendation-service | 8095 | analytics-service, delivery-service, user-service, cache-service, Redis, Kafka |
| delivery-service | 8096 | notification-service, warehouse-service, email-service |
| pricing-service | 8097 | discount-service, tax-service, cache-service, Redis |
| discount-service | 8098 | loyalty-service, cache-service |
| user-service | 8099 | cache-service |
| notification-service | 8100 | email-service, Kafka |
| cart-service | 8101 | catalog-service, pricing-service, session-service, Redis |
| catalog-service | 8102 | review-service, media-service, cache-service, pricing-service, inventory-service |
| review-service | 8103 | recommendation-service, user-service, media-service, Kafka |
| loyalty-service | 8104 | user-service, cache-service, Redis |
| audit-service | 8105 | Kafka |
| session-service | 8106 | Redis |
| analytics-service | 8107 | reporting-service, cache-service, Kafka |
| email-service | 8108 | (leaf) |
| cache-service | 8109 | Redis |
| reporting-service | 8110 | cache-service |
| fraud-detection | 8111 | analytics-service, Redis |
| tax-service | 8112 | (leaf) |
| warehouse-service | 8113 | Kafka |
| media-service | 8114 | cache-service |
| external-payment-api | 8115 | (leaf, simulated external) |

### Scaling (HPA)

12 high-traffic services have HPA enabled (min 1, max 10, target 70% CPU):

`frontend`, `api-gateway`, `checkout`, `payments-api`, `search-service`, `orders-service`, `catalog-service`, `user-service`, `auth-service`, `cart-service`, `recommendation-service`, `ranking-service`

All services start at **1 replica**. HPA will scale them up to 10 under CPU load (≥70%). Total pod count at baseline: **~43 pods** (36 services + 3 data stores + 4 observability).

### Data Stores

| Store | Port | Purpose |
|-------|------|---------|
| payments-db (PostgreSQL 16) | 5432 | Payment transaction storage |
| Redis 7 | 6379 | Cart cache, session store, ranking cache, inventory cache, loyalty, pricing, fraud rate tracking, centralized cache |
| Kafka (Confluent 7.6.0, KRaft) | 9092 | Async event streaming (8 topics) |

**Observability stack:**
- Prometheus — scrapes all 36 services + postgres-exporter + redis-exporter; evaluates alerting rules every 15s
- Grafana — pre-built dashboard (login: admin / admin)

**Alerting rules** (`environment/alert_rules.yml`, embedded in `k8s/01-configmaps.yaml`):

| Alert | Scenario | Severity | Condition |
|-------|----------|----------|-----------|
| `CheckoutHighErrorRate` | 3, 4 | critical | checkout 5xx > 1% for 30s |
| `PaymentsAPIHighErrorRate` | 3, 4 | critical | payments-api 5xx > 1% for 30s |
| `PaymentsDBQueryLatencyHigh` | 3, 4 | critical | DB query p95 > 500ms for 30s |
| `RecommendationServiceLatencyHigh` | 5, 6, 7 | critical | recommendation-service p95 > 500ms for 30s |
| `ReviewServiceLatencyHigh` | 5, 6, 7 | warning | review-service p95 > 500ms for 30s |
| `CatalogServiceLatencyHigh` | 5, 6, 7 | warning | catalog-service p95 > 500ms for 30s |
| `FrontendCatalogLatencyHigh` | 5, 6, 7 | critical | frontend p95 > 1s for 30s |
| `ServiceHighErrorRate` | all | warning | any service 5xx > 1% for 30s |
| `ServiceHighLatencyP95` | all | warning | any service p95 > 500ms for 30s |

Rules are evaluated by Prometheus; no Alertmanager is configured, so alerts are visible in the Prometheus `/alerts` UI and queryable via `ALERTS{}` metric. The `for: 30s` pending period fits within the 60-second `FAULT_SETTLE_SECONDS` wait the eval runner uses after fault injection.

---

## Environment

### Prerequisites
- Kubernetes cluster (minikube, kind, k3s, EKS, GKE, etc.) + kubectl
- Docker (for building the Go service images — no local Go installation needed)
- Python 3.10+ (required for the local load generator and the MCP eval)

#### Python setup

Create and activate the virtual environment once, then use `python` / `pip` for all commands in this README:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r load/requirements.txt pyyaml
```

> Re-activate in new shells: `source .venv/bin/activate`

#### kind cluster setup

If you don't have a cluster, kind is the fastest way to get started locally:

```bash
# Install kind (macOS)
brew install kind

# Create a cluster
kind create cluster --name scenario-01

# Verify kubectl is pointing at it
kubectl cluster-info --context kind-scenario-01
```

### 1. Build service images

All 36 Go services use multi-stage Docker builds (`golang:1.25-alpine` → `alpine:3.19`)
and need their images built and made available to your cluster.

Images are pushed to `docker.io/causely-oss/<service>:v1` by default. To use a different
registry or tag, set the `REGISTRY` and `TAG` environment variables:

```bash
docker login
bash k8s/build-images.sh                          # default: docker.io/causely-oss/*:v1 (linux/amd64,linux/arm64)
REGISTRY=docker.io/myuser TAG=v2 bash k8s/build-images.sh  # example custom registry/tag
```

The default platform list is `linux/amd64,linux/arm64`. That is required for the current GKE nodes (`arm64`). To build one arch only: `PLATFORM=linux/arm64 bash k8s/build-images.sh`.

If you change the registry or tag, update `k8s/03-app.yaml` to match:
```bash
sed -i '' 's|image: docker.io/causely-oss/|image: docker.io/<your-username>/|g' k8s/03-app.yaml
sed -i '' 's|:v1|:<your-tag>|g' k8s/03-app.yaml
```

**kind — loading locally built images:** kind nodes run in Docker containers and can't see your local Docker images directly. After building, load each image into the cluster:

```bash
# Load all 36 images into kind (replace causely-oss / v1 if you used a custom registry/tag)
for svc in $(docker images --format '{{.Repository}}:{{.Tag}}' | grep 'causely-oss'); do
  kind load docker-image "$svc" --name scenario-01
done
```

Alternatively, push to a registry (Docker Hub, GHCR, local registry) and skip the load step — kind will pull normally.

### Cart Service Version Switching

`kubectl apply -f k8s/` deploys cart-service **v1** from `k8s/03-app.yaml`. The v2 overlay lives in `k8s/optional/` so it is **not** included in that apply (kubectl does not recurse into subdirectories).

v1 and v2 are identical except for the image tag. Applying v2 replaces the same Deployment via a rolling update. Build and push `:v2` first (`TAG=v2 bash k8s/build-images.sh`), then:

**Deploy v2:**
```bash
kubectl apply -f k8s/optional/cart-service-v2.yaml
```

**Switch back to v1:**
```bash
kubectl apply -f k8s/03-app.yaml
```

**Confirm which version is running:**
```bash
kubectl get deployment cart-service -n scenario-01 -o jsonpath='{.spec.template.spec.containers[0].image}'
```

Or for a wider view including rollout status:
```bash
kubectl get deployment cart-service -n scenario-01 -o wide
```

---

### 2. Deploy the stack

**minikube / kind (local)** — all services start at 1 replica, cart-service on v1:
```bash
kubectl apply -f k8s/
```

This applies every manifest in `k8s/*.yaml` (namespace, configmaps, data stores, app, loadgen, observability, HPA). It does **not** apply `k8s/optional/` (cart-service v2).

> **kind note:** if you loaded images locally (see above), the manifests use `imagePullPolicy: IfNotPresent` by default so the pre-loaded images are used without hitting the registry.

> **Prometheus:** this stack does not run a Prometheus Deployment in `scenario-01`. Apply creates a `PodMonitor` / `ScrapeConfig` and `PrometheusRule` that kube-prometheus-stack in `monitoring` picks up. No rollout restart is needed.

> **Traces and logs:** services export OTLP to the in-namespace `otel-collector` (same hop as `otel-demo`: app → `otel-collector:4318`). Traces go to Tempo and `mediator.observability-staging:4317` — the same OTLP endpoint otel-demo’s collector uses (`otlp/causely`). Metrics also go to `mediator.observability-chaos`. Logs stay on stdout; Promtail in `monitoring` ships them to Loki.

> **kafka-exporter image:** The kafka-exporter uses a locally built image (`kafka-exporter:kind`) that must be loaded into kind before deploying. If the `kafka-exporter` pod shows `ImagePullBackOff`, run:
> ```bash
> docker pull danielqsj/kafka-exporter:latest
> docker buildx build --platform linux/amd64 --provenance=false --load -t kafka-exporter:kind - <<'EOF'
> FROM danielqsj/kafka-exporter:latest
> EOF
> kind load docker-image kafka-exporter:kind --name kind
> ```

**EKS / GKE / production cluster** — apply the same default stack, then scale the 12 high-traffic services to 3 replicas:
```bash
kubectl apply -f k8s/
bash k8s/production-scale.sh
```

`production-scale.sh` sets `replicas=3` and `HPA minReplicas=3` for: `frontend`, `api-gateway`, `checkout`, `payments-api`, `search-service`, `orders-service`, `catalog-service`, `user-service`, `auth-service`, `cart-service`, `recommendation-service`, `ranking-service`.

Wait ~90 seconds for all pods to become ready:

```bash
kubectl get pods -n scenario-01 -w
```

### 3. Access services

> **Note:** The application services are JSON APIs — there is no HTML UI to browse.
> The only browser UIs are **Grafana** and **Prometheus**.


The node IP is not routable from the host on these platforms. Use port-forward:

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80 &
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090 &
kubectl port-forward -n scenario-01 svc/frontend 8080:8080 &
```

Then open:
- Grafana: http://localhost:3000 (shared stack in `monitoring`)
- Prometheus: http://localhost:9090 (targets should include `scenario-01-pods`)

And use `http://localhost:8080` as the frontend URL for the load generator.


### Grafana dashboard

Navigate to **Dashboards → Scenario 01 — Full Platform (36 services)** (login: admin / admin).

The dashboard has 10 sections with 34 panels:

| Section | Key Panels |
|---------|-----------|
| Platform Health | Checkout success rate, platform error rate, payments-api p95, DB query duration, DB connections, Redis memory |
| Request Rate — All Services | Single timeseries showing req/s for all 36 services |
| Error Rates by Flow | Checkout pipeline error %, search/catalog/auth error % |
| Latency p95 by Flow | Checkout flow p95, deep call chain p95 (catalog→review→rec→analytics→reporting) |
| Auth · Search · Orders | Auth, search, and order pipeline request rates |
| Billing · Streaming · Support | Billing flow, streaming flow, support services request rates |
| Database · Redis · Infra | DB query duration, connections, Redis stats |
| Latency Percentiles | Checkout and payments-api latency percentiles (p50/p95/p99) |
| Error Rate — All Services | Single timeseries showing error % for all 36 services |
| p95 Latency — All Services | Single timeseries showing p95 latency for all 36 services |

Baseline targets:
- Checkout success rate: 99.5%
- payments-api p95 latency: <50ms
- payments-db query duration: ~20ms
- All services error rate: <1%

---

## OPTIONAL Configure Causely 

After deploying the stack, configure [Causely](https://causely.ai) labels on all services
to set a 50ms latency threshold and a 1-minute delayed activation for both error rate and
latency symptoms:

```bash
bash configure_causely.sh
```

This applies three labels to every service in the `scenario-01` namespace:

| Label | Value | Purpose |
|-------|-------|---------|
| `causely.ai/latency-threshold` | `450.0` | Flag requests slower than 50ms |
| `causely.ai/error-rate-activation-delay` | `1` | Wait 1 minute before activating error rate symptoms |
| `causely.ai/latency-activation-delay` | `1` | Wait 1 minute before activating latency symptoms |

Verify labels:

```bash
kubectl get svc -n scenario-01 --show-labels
```

---

## Load Generation

### Option A: In-cluster (recommended)

The load generator runs as a Deployment inside the cluster, sending traffic directly
to the frontend Service. This avoids port-forward bottlenecks and handles the full
~154 rps without errors.

```bash
kubectl apply -f k8s/04-loadgen.yaml
kubectl logs -f -l app=load-generator -n scenario-01
```

Stop: `kubectl delete -f k8s/04-loadgen.yaml`

### Option B: Local

Useful for quick tests or when you want to adjust RPS interactively.
Requires a port-forward to the frontend and may not sustain full RPS.

```bash
pip install -r load/requirements.txt
kubectl port-forward -n scenario-01 svc/frontend 8080:8080 &
python load/generate_load.py --frontend-url http://localhost:8080
```

Stop: `Ctrl+C`

The load generator drives 8 workloads simultaneously, covering all 36 services:
- **Checkout** (5 rps): checkout → pricing → discount → loyalty; → billing → payment-adapter → external-payment-api; → payments-api → payments-db; → fraud-detection
- **Browse** (100 rps): fast local product listings
- **Search** (20 rps): search → ranking → profile → user-service → cache-service; ranking → analytics, recommendation
- **Auth** (3 rps): auth → session → user-service → Kafka → audit-service
- **Catalog** (8 rps): deep chain: catalog → review → recommendation → analytics → reporting (+ fan-out to media, cache, pricing, inventory, user, delivery, notification, email)
- **Orders** (5 rps): orders → inventory → warehouse → Kafka → shipping → notification → email
- **Cart** (10 rps): cart → catalog → pricing; cart → session-service
- **Ingest** (3 rps): ingest → Kafka → processing → Kafka → recommendation → delivery

Total: ~154 rps baseline across all 36 services.

Live summary every 10 seconds:

```
[10:00:00] chk=  50 ( 99%) | brw=1000 | sch= 200 | auth=  30 | cat=  80 | ord=  50 | cart= 100 | ing=  30 | rps= 154 err=1
```

Leave load running before injecting the issue.

---

## Failure Injection Scenarios

All injections are independent — run one at a time with load active. Each has an inject and restore script. Allow ~60 seconds for metrics to propagate after injection.

**Restore everything at once:**
```bash
bash inject/restore_all.sh
```

### Scenario 0: DB Latency Spike (default)

```bash
bash inject/inject_db_latency.sh      # inject
bash inject/restore_db.sh             # restore
```

**Root cause:** `payments-db` query latency: 20ms → 2,800ms
**Effects:** DB CPU ~92%, payments-api error rate ~18%, checkout error rate ~14%
**Eval prompt:** *"What is causing checkout failures?"*

### Scenario 1: Inventory Consumer Stall

```bash
bash inject/inject_inventory_stall.sh      # inject
bash inject/restore_inventory_stall.sh     # restore
```

**Root cause:** inventory-service Kafka consumer paused — stops processing `regression-lab-inventory-updates`
**Effects:** Kafka consumer lag grows to 80k+, orders-service times out waiting on inventory, shipping pipeline stalls
**Red herrings:** Kafka brokers are healthy, orders-service latency appears first
**Eval prompt:** *"Orders are taking forever and the shipping pipeline seems stuck."*

### Scenario 2: CPU Throttling (Search + Ranking)

```bash
bash inject/inject_cpu_throttle.sh      # inject
bash inject/restore_cpu_throttle.sh     # restore
```

**Root cause:** search-service and ranking-service CPU throttled (simulated via latency + errors)
**Effects:** search p95 ~800ms, ranking p95 ~500ms, search errors ~8%, frontend /search >1.5s
**Red herrings:** profile-service and user-service appear slow in traces but are healthy
**Eval prompt:** *"Search is extremely slow and returning errors."*

### Scenario 3: External API Degradation

```bash
bash inject/inject_external_api_latency.sh      # inject
bash inject/restore_external_api_latency.sh     # restore
```

**Root cause:** external-payment-api latency 150ms → 3s, error rate 2% → 6%
**Effects:** payment-adapter retries amplify latency, billing-service latency spikes to 6s+, checkout latency increases
**Red herrings:** payments-api and payments-db are healthy, billing looks like the problem
**Eval prompt:** *"Billing is taking forever and some charges are failing."*

### Scenario 4: Redis Memory Pressure

```bash
bash inject/inject_redis_pressure.sh      # inject
bash inject/restore_redis_pressure.sh     # restore
```

**Root cause:** Redis maxmemory reduced to 2MB → mass evictions under allkeys-lru
**Effects:** cache hit rate 92% → 45%, all Redis-dependent services see latency increase (cart, session, ranking, pricing, fraud, inventory, loyalty, recommendations), DB load increases from cache misses
**Red herrings:** many services show latency increase simultaneously, DB load rises
**Eval prompt:** *"Everything is slow — auth, checkout, search, recommendations all degraded."*

### Scenario 5: Elevated Pod Errors (Checkout)

```bash
bash inject/inject_pod_errors.sh      # inject
bash inject/restore_pod_errors.sh     # restore
```

**Root cause:** checkout service application errors (20% error rate)
**Effects:** checkout success rate 99.5% → 80%, error messages in checkout pod stdout, api-gateway error rate rises
**Red herrings:** payments-api is healthy, errors happen before the payment call
**Eval prompt:** *"Checkout is failing for about 1 in 5 users."*

### Scenario 6: Orders Retry Loop → Inventory Overload

```bash
bash inject/inject_orders_retry_loop.sh      # inject
bash inject/restore_orders_retry_loop.sh     # restore
```

**Root cause:** inventory-service errors (40%) cause orders-service retry amplification
**Effects:** inventory error rate ~40% + 800ms latency, orders-service latency 50ms → 2s, inventory logs filled with errors
**Red herrings:** orders-service latency spike appears first in metrics, Kafka looks healthy
**Eval prompt:** *"Orders are slow and I'm seeing errors in the inventory service logs."*

### Scenario 7: Discount-Service Latency Spike

```bash
bash inject/inject_discount_latency.sh      # inject
bash inject/restore_discount_latency.sh     # restore
```

**Root cause:** discount-service latency 5ms → 800ms (no traces available)
**Effects:** pricing-service latency ~850ms, checkout step 2 adds ~800ms, overall checkout slower
**Red herrings:** pricing-service looks slow, loyalty-service appears affected, payments path is clean
**Eval prompt:** *"Checkout is slower than usual but not failing. What's going on?"*

### Scenario 8: Deep Call Chain Latency

```bash
bash inject/inject_deep_chain_latency.sh      # inject
bash inject/restore_deep_chain_latency.sh     # restore
```

**Root cause:** recommendation-service latency 50ms → 600ms
**Chain:** frontend → api-gateway → catalog-service → review-service → recommendation-service → analytics-service → reporting-service (6 hops)
**Effects:** every upstream service shows moderate latency increase (~+550ms cascade), frontend /catalog p95 >1s
**Red herrings:** all 6 services in the chain show latency, analytics and reporting appear slow
**Eval prompt:** *"The product catalog pages are loading very slowly."*

---

## Regression Scenarios (code/config fix required)

The scenarios above are runtime toggles — the fix is a curl call to a
restore script, not a diff. `scenarios/` holds a separate set of scenarios
where the regression is a real commit to application code or a Kubernetes
manifest (as if a bad PR had merged), and the fix has to be an actual PR, not
a flag flip. See [`scenarios/README.md`](scenarios/README.md) for the full
model; scenarios currently defined:

| # | Slug | Fix type | Service |
|---|------|----------|---------|
| 9 | [billing-missing-timeout](scenarios/09-billing-missing-timeout/README.md) | App code | `billing-service` |
| 10 | [recommendation-oom](scenarios/10-recommendation-oom/README.md) | K8s config | `recommendation-service` |
| 11 | [pricing-n-plus-one](scenarios/11-pricing-n-plus-one/README.md) | App code | `pricing-service` |

---

## Teardown

```bash
kubectl delete namespace scenario-01
```

---

## MCP Eval — Grafana vs Grafana + Causely

Benchmarks Claude's diagnostic accuracy, latency, token usage, and tool calls with and without the Causely MCP, using two fault scenarios and two healthy-baseline prompts.

### Prerequisites

**1. Port-forwards running:**
```bash
kubectl port-forward -n scenario-01 svc/prometheus 9090:9090 &
kubectl port-forward -n scenario-01 svc/grafana    3000:3000 &
```

**2. Causely MCP authenticated** (required for condition B):
Open a Claude Code session and run `/mcp`, select **claude.ai Causely**, complete the OAuth flow.

**3. Load generation running:**
```bash
kubectl apply -f k8s/04-loadgen.yaml
```

### Conditions

| Condition | Tools available |
|-----------|----------------|
| A | Local Grafana MCP only (`mcp__grafana__*`) |
| B | Local Grafana MCP + Causely MCP (`mcp__claude_ai_Causely__*`) |

### Running the eval

```bash
# Full run — all 7 prompts × 2 conditions (~60–90 min including fault injection waits)
python eval/run_eval.py

# Subset — run specific conditions or prompts
python eval/run_eval.py --conditions A          # Grafana only
python eval/run_eval.py --conditions B          # Causely only
python eval/run_eval.py --prompts 3,4           # DB latency scenario only
python eval/run_eval.py --conditions A --prompts 1,2  # healthy baseline, condition A

# Preview prompts without calling the API
python eval/run_eval.py --dry-run
```

Results are written to `eval/results/run_<timestamp>.jsonl`.

### Scoring and report

```bash
# Score responses interactively (0 = wrong, 1 = partial, 2 = correct), then print table
python eval/report.py eval/results/run_<id>.jsonl --score

# Print table for an already-scored file
python eval/report.py eval/results/run_<id>.jsonl
```

The report shows per-prompt accuracy, latency, token count, and tool call count side-by-side for A vs B, plus aggregate deltas.

### Prompts

| # | State | Fault | Question |
|---|-------|-------|---------|
| 1 | Healthy | — | Platform health summary |
| 2 | Healthy | — | Highest p95 latency service |
| 3 | Fault | DB latency | "What is causing checkout failures?" |
| 4 | Fault | DB latency | "payments-api errors have spiked. Identify the root cause." |
| 5 | Fault | Deep chain | "The product catalog pages are loading very slowly." |
| 6 | Fault | Deep chain | "I see elevated latency across multiple catalog services. What's the origin?" |
| 7 | Fault | Deep chain | "Frontend /catalog p95 is over 1 second. Walk me through the call chain." |

Fault injection and restore are handled automatically by the runner between prompts.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Services (36) | Go 1.25, `net/http` |
| Prometheus metrics | `github.com/prometheus/client_golang` |
| Kafka | `github.com/IBM/sarama` |
| Redis | `github.com/redis/go-redis/v9` |
| PostgreSQL | `github.com/jackc/pgx/v5` |
| Container images | Multi-stage: `golang:1.25-alpine` → `alpine:3.19` |
| Service generator | `generate_services.py` (generates Go code for 32 of 36 services) |
| Load generator | Python 3 + aiohttp |

4 services have hand-written Go code (`frontend`, `api-gateway`, `checkout`, `payments-api`).
The remaining 32 are generated by `generate_services.py`.

## Docker Compose Alternative

For local development without Kubernetes:

```bash
cd environment/
docker compose up --build -d
```

This starts all 36 services, data stores, and observability stack locally.

---

## Expected Agent Behavior

A well-performing on-call agent should:

1. Identify the elevated checkout error rate as the user-facing symptom
2. Trace the error upstream through the service mesh: checkout → payments-api → payments-db
3. **Not get distracted** by the 32 other healthy services, Kafka pipelines, or the deep call chain
4. Observe the DB query latency spike (20ms → 2.8s)
5. Note DB CPU saturation as a contributing factor (not the root cause)
6. Conclude: **payments-db query latency spike is causing the checkout failures**
7. Suggest: check for missing indexes, long-running queries, or a recent schema/data change on `payments-db`

The agent should **not** blame Redis, Kafka, search, auth, billing, or any of the other services — those are healthy. The complexity of 36 services with cross-cutting communication is intentional: it tests whether the agent can focus on the actual signal amid noise.
