# Lesson 16 — Deployment: Flink Kubernetes Operator

The Flink Kubernetes Operator is the recommended way to run Flink on
Kubernetes in production. It introduces two CRDs:

- **`FlinkDeployment`** — defines a Flink cluster. With a `job:` block
  it's an application-mode cluster (one job per cluster). Without, it's
  a session cluster (long-lived, takes many jobs).
- **`FlinkSessionJob`** — attaches a job to a session cluster (defined
  by a FlinkDeployment with no `job:` block).

The operator reconciles your declarative YAML against actual cluster
state — savepoint on upgrade, rollback on crashloop, autoscaler
integration, exposed metrics. Compared to manually running
`flink run-application -t kubernetes-application`, the operator
gives you the GitOps loop most platform teams want.

## How this lesson works

There's no Java code to run. Read the manifests in `manifests/` in
order and follow the discussion below.

```bash
ls manifests/
# 01-flinkdeployment.yaml          # application-mode (one job)
# 02-flinksessionjob.yaml          # job submitted to a session
# 03-flinkdeployment-session.yaml  # the session cluster itself
```

## Prerequisite: install the operator

```bash
helm repo add flink-operator-repo \
    https://downloads.apache.org/flink/flink-kubernetes-operator-1.7.0/
helm install flink-operator \
    flink-operator-repo/flink-kubernetes-operator \
    --namespace flink-operator-system --create-namespace
```

The operator runs as a single deployment in `flink-operator-system`
and watches `FlinkDeployment` / `FlinkSessionJob` resources across the
cluster.

---

## Stage 1 — `FlinkDeployment` (application mode)

Read `manifests/01-flinkdeployment.yaml`. Things to notice:

1. **`spec.image`** points to YOUR job's image — not the upstream
   Flink image. The image inherits FROM `flink:1.18.1` and bakes in
   `/opt/flink/usrlib/your-job.jar`. The operator is *not* a magic
   "give me your jar" platform; you build the image yourself.

2. **`spec.flinkConfiguration`** is `flink-conf.yaml` as a map. Every
   key from Lesson 15 fits here. Common gotcha: values must be quoted
   strings in YAML. `state.backend.incremental: "true"`, not
   `true` (which YAML parses as boolean).

3. **`spec.job.upgradeMode: savepoint`** — when you change the image
   tag and `kubectl apply` again, the operator:
   - takes a savepoint of the running job
   - tears down the old cluster
   - starts the new cluster from the savepoint

   Other modes:
   - `last-state` — uses last completed checkpoint (no extra savepoint
     latency, but checkpoints might be older).
   - `stateless` — discards state. Use for stateless jobs (filter →
     transform → sink) only.

4. **`spec.job.state: running`** — declarative. Set to `suspended` to
   stop the job (operator takes a final savepoint). Set back to
   `running` to resume from that savepoint. **Never** `kubectl delete`
   to stop a job — that loses state.

> *Bonus question:* What happens if the new image crashloops on
> startup?
>
> *Answer:* The operator detects that the new JobManager pod is failing
> to come up, marks the deployment status as `LAST_STABLE_SPEC_ERROR`,
> and rolls back to the previous image — **with the savepoint that
> was taken before the upgrade**. So a bad image deploys with zero
> state loss. This is the killer feature.

---

## Stage 2 — `FlinkSessionJob` (session mode)

Read `manifests/02-flinksessionjob.yaml` and
`manifests/03-flinkdeployment-session.yaml` together.

The flow:
1. Apply `03-flinkdeployment-session.yaml` once — creates a long-lived
   session cluster with 3 TMs × 4 slots = 12 slots total.
2. Apply `02-flinksessionjob.yaml` — submits a job to that session
   cluster, claiming `parallelism: 2` slots.
3. Apply more `FlinkSessionJob`s as needed; they share the slots.

Use cases for session mode:

- **SQL Gateway** — many small ad-hoc SQL queries from many users.
- **Mini-batch backfills** — quick jobs that don't justify a full
  cluster lifecycle.
- **Test/dev** — share one cluster across feature branches.

Don't use session mode for:

- **Mission-critical pipelines** — one bad job can take down the
  cluster, and the session cluster's classloader leaks make
  restarts expensive.
- **Jobs with very different memory profiles** — a RocksDB-heavy
  job and a stateless job in the same TM JVM is misery.

> *Bonus question:* Why does `FlinkSessionJob` upload the jar via URL
> rather than local path?
>
> *Answer:* The operator runs in its own pod, separate from the
> session JobManager. It can't access "local files" — it has to
> download the jar over HTTP/HTTPS or pull from a shared volume. URL
> uploads also let you ship JAR builds via your CI artifact
> registry without rebuilding the cluster image.

---

## Stage 3 — Operator-driven autoscaling

The operator (since 1.5) ships a job autoscaler:

```yaml
spec:
  flinkConfiguration:
    job.autoscaler.enabled: "true"
    job.autoscaler.metrics.window: "10m"
    job.autoscaler.target.utilization: "0.7"
    job.autoscaler.target.utilization.boundary: "0.2"
    job.autoscaler.scale-up.grace-period: "10m"
    pipeline.max-parallelism: "1024"
```

The autoscaler reads operator-level metrics (`busyTimeMsPerSecond`,
`backPressuredTimeMsPerSecond`), targets ~70% utilization, and adjusts
parallelism per **operator** (not per job — different operators may
need different parallelism).

It triggers a **savepoint+restore** to apply the new parallelism, so
the user-visible effect is an interruption of a few tens of seconds
once the savepoint completes.

> *Bonus question:* Why does autoscaling require a max parallelism
> hint up front?
>
> *Answer:* Flink's keyed state is sharded across `pipeline.max-parallelism`
> key groups (default 128). To scale beyond that, you'd need to
> reshard state — possible but expensive. So you set
> `pipeline.max-parallelism` once, sized for your peak (e.g., 1024),
> and the autoscaler can move parallelism freely below that ceiling
> with cheap savepoint-restore cycles. Setting it too low is a
> common production mistake.

---

## Stage 4 — Theory: GitOps loop

The win of the operator over manual `flink run`:

```
git push ──▶ argo/flux ──▶ kubectl apply ──▶ operator reconciles ──▶ job upgrades
```

Every change to the pipeline — image tag, parallelism, config —
flows through git. The operator handles the savepoint dance. You
never `flink stop --savepointPath ... <jobid>` by hand.

The trade-off: **the operator takes time to settle**. A typical
upgrade is 30-90s (savepoint + cluster restart + state restore).
For a low-latency pipeline that can't tolerate a 60s window, you'd
use blue/green at the application level (run two jobs writing to
different sinks; swap consumers).

> *Bonus question:* What does the operator do that you'd otherwise have
> to script yourself?
>
> *Answer:* Savepoint cleanup (delete savepoints older than N), HA
> ConfigMap garbage collection, leader election handoff during
> JobManager restarts, RBAC for service accounts, metrics
> aggregation, and rollback on bad deploys. Each is a single line of
> YAML; without the operator, each is a 100-line bash script.

---

## When you're done

Read all three manifests. Move on to **Lesson 17 — SQL-centric
Deployment**.
