# Lesson 23 — Workshop: Production-Ready Pipeline in Kubernetes

This is the capstone. You've covered:

- DataStream API (Lessons 1-4)
- Table & SQL (Lessons 5-7)
- Connectors and operators (Lessons 8-9)
- Performance and skew (Lessons 10-13)
- Pipeline design (Lesson 14)
- Deployment, K8s operator, SQL deployment (Lessons 15-17)
- Reliability (Lesson 18)
- Observability (Lesson 19)
- Control plane (Lesson 20)
- Tuning, benchmarking (Lessons 21-22)

This workshop ties them all together as a single deployable system.

## How this lesson works

Read the four manifests in `manifests/` in order. They form one
production-grade deployment:

```
01-namespace-rbac.yaml    → namespace + service account + role binding
02-secrets.yaml           → credentials (placeholder; real ones come from a secret operator)
03-flinkdeployment.yaml   → the FlinkDeployment with all the right settings
04-monitoring.yaml        → ServiceMonitor + alert rules
```

Apply order:

```bash
kubectl apply -f manifests/01-namespace-rbac.yaml
kubectl apply -f manifests/02-secrets.yaml
kubectl apply -f manifests/03-flinkdeployment.yaml
kubectl apply -f manifests/04-monitoring.yaml
```

---

## Stage 1 — Production checklist

Walk through `03-flinkdeployment.yaml` and confirm every item:

| Concern | Setting | Lesson |
| --- | --- | --- |
| Stateful upgrades | `upgradeMode: savepoint` | 16 |
| HA (no JM SPOF) | `jobManager.replicas: 2` + `high-availability.type: kubernetes` | 18 |
| Durable checkpoints | `state.checkpoints.dir: s3://...` | 18 |
| Fast checkpoints | `unaligned: true`, `incremental: true` | 18, 21 |
| RocksDB tuning | `predefined-options: FLASH_SSD_OPTIMIZED`, `timer-service.factory: ROCKSDB` | 21 |
| Network buffers | `buffers-per-channel: 8` | 21 |
| State TTL | `table.exec.state.ttl: 12h` | 7, 17 |
| MiniBatch (skew) | `table.exec.mini-batch.enabled: true` | 12, 17 |
| Autoscaling | `job.autoscaler.enabled: true` | 16 |
| Max parallelism set | `pipeline.max-parallelism: 1024` | 16 |
| Metrics export | `metrics.reporters: prom` + ServiceMonitor | 19 |
| Alerts | PrometheusRule with paging + warning rules | 19 |
| Credentials | Secret + envFrom (no plaintext) | 23 |
| Image-tag deploys | Via GitOps; image fully qualified | 16 |
| Local recovery | `state.backend.local-recovery: true` (in rocksdb-tuned.yaml) | 21 |
| Ephemeral storage | `resources.requests.ephemeral-storage: 10Gi` | 23 |

If any line in your real-world pipeline doesn't match this list,
that's a probable production risk.

---

## Stage 2 — The four alerts

Every Flink job should have at least these:

1. **`FlinkJobDown`** — the most basic. Fires when Prometheus can't
   scrape the JM. 5min for-window so flapping during JM restart
   doesn't page.

2. **`FlinkCheckpointFailing`** — if multiple checkpoints fail in
   10min, something is wrong (timeout, S3 issues, state corruption).
   Pages immediately because state is at risk.

3. **`FlinkBackpressureSustained`** — `>600 ms/sec` of backpressure
   for 15min means an operator is the bottleneck. Warning level
   because it's about throughput, not data loss.

4. **`FlinkCheckpointDurationTooLong`** — avg > 5min for 15min.
   Indicates state is growing or RocksDB is slow. Warning.

> *Bonus question:* Why aren't `numRecordsInPerSecond` lag alerts on
> this list?
>
> *Answer:* Throughput-based alerts have too many false positives —
> traffic naturally varies hour to hour. The right replacement is
> "Kafka consumer lag exceeds N" alerts on the **broker side**
> (Burrow, Confluent Control Center). The Flink-side metric is
> "are we processing what we receive?" which is captured by the
> backpressure alert.

---

## Stage 3 — The 30-minute production diagnosis runbook

When you get paged at 2am:

1. **Open the Flink UI.** Go to "Job Graph". Find the first red
   operator from the source.
2. **Open the operator's subtask detail.** Compare per-subtask
   `numRecordsInPerSecond`. If wildly uneven, you have skew (Lesson 12).
3. **If even but slow, check the operator's metrics.** Look at
   `busyTimeMsPerSecond`. Close to 1000? CPU-bound. Run
   `flame.html` (Lesson 22) — find the hot path.
4. **If neither, check the sink.** Sink saturation propagates
   backwards. Sink-side metrics (Postgres CPU, Kafka producer
   `acks-rate`) are external to Flink.
5. **Check checkpoint status.** Stuck or slow checkpoints can stall
   sources via `execution.checkpointing.tolerable-failed-checkpoints`
   default of 0 — one slow checkpoint and downstream halts.
6. **Last resort: take a savepoint and restart.** If state is
   confused (e.g., a watermark generator is stuck), a clean restart
   from savepoint clears transient operator state.

> *Bonus question:* When is "scale up" the right answer?
>
> *Answer:* When `busyTimeMsPerSecond` is at 1000 across MOST
> subtasks (i.e., evenly distributed CPU saturation, not skew). At
> that point you have genuinely more work than your current
> parallelism can do. Otherwise scaling up just gives you more idle
> subtasks.

---

## Stage 4 — Theory: things this workshop did NOT cover

To keep the workshop tractable, we skipped:

- **Network policies** — restricting pod-to-pod traffic. Production
  clusters always have these; the FlinkDeployment Service shape needs
  matching `NetworkPolicy` rules to allow JM/TM traffic + Prometheus
  scrape.
- **Pod disruption budgets** — preventing K8s from evicting all your
  TMs at once during cluster maintenance. Add a PDB with
  `maxUnavailable: 1` per TaskManager StatefulSet.
- **Image scanning** — Trivy/Grype/Snyk on every image build.
- **Resource quotas** — Namespace-level limits so a runaway autoscaler
  doesn't take the cluster down.
- **Audit logging** — every CRD apply, every savepoint, who/when.
  Standard Kubernetes audit log + a Flink-specific event ledger.
- **Multi-region DR** — savepoint replication to a second region; a
  cold-standby cluster ready to start from the latest replicated
  savepoint. Lesson 20 mentioned this as a control-plane responsibility.

If you need any of these, the patterns are well-established outside
Flink — apply your existing Kubernetes platform's playbook.

---

## When you're done

You've built a production-ready Flink pipeline configuration. The
manifests in `manifests/` are a useful starting template — copy them
into your own job's repo, swap names + image tags, and you have 80%
of a deploy.

This concludes the **Flink in Production** section.

---

## What's next

You've completed all the curriculum-defined lessons. Real production
practice is where the curriculum ends and the real learning begins:

- Pick a real pipeline (synthetic or otherwise) and put it through
  the full lifecycle.
- Subscribe to the Apache Flink user mailing list.
- Read the Flink source for any operator you depend on — the inline
  comments are gold.
