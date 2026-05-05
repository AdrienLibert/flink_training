# Lesson 20 — Building a Control Plane

A "control plane" sits between your operators (or CI) and the Flink
cluster. It's responsible for things the cluster itself doesn't do:

- Savepoint cleanup (how many to keep, how old, where).
- Job upgrade orchestration (stop with savepoint → upload new jar →
  submit from savepoint).
- Cluster lifecycle (start/stop, scale up/down).
- Multi-cluster routing (which job goes on which cluster).
- Fleet-wide policy enforcement (every job must have HA, must
  checkpoint to S3, must report Prometheus metrics, etc.).

The Flink Kubernetes Operator (Lesson 16) is the most common control
plane today. Sometimes you need your own.

## How this lesson works

There's no Java. Read:

- `scripts/upgrade-with-savepoint.sh` — bash that does what the
  operator does, using only the REST API.
- The "REST endpoints worth knowing" section below.

---

## Stage 1 — The REST API

Flink's JobManager exposes a JSON REST API on port 8081 (default).
Every action you can do from the UI or `flink` CLI is also a REST call.

| Endpoint | Purpose |
| --- | --- |
| `GET /overview` | Cluster status (slots free, taskmanagers up) |
| `GET /jobs` | List all running jobs |
| `GET /jobs/<id>` | Job details (operator graph, status) |
| `GET /jobs/<id>/metrics` | Job-level metrics |
| `POST /jobs/<id>/savepoints` | Trigger a savepoint (does NOT stop) |
| `POST /jobs/<id>/stop` | Stop with savepoint (graceful) |
| `PATCH /jobs/<id>?mode=cancel` | Hard cancel (no savepoint!) |
| `POST /jars/upload` | Upload a new job jar |
| `GET /jars` | List uploaded jars |
| `POST /jars/<id>/run` | Submit a job from an uploaded jar |
| `GET /checkpoints` | List checkpoints |
| `POST /jobs/<id>/checkpoints/<id>` | Inspect a specific checkpoint |

The crucial pair: **`stop` + `run` with `savepointPath`**. The
upgrade script in `scripts/` is exactly this dance.

> *Bonus question:* What's the difference between `POST /jobs/<id>/stop` and
> `PATCH /jobs/<id>?mode=cancel`?
>
> *Answer:* `stop` triggers a savepoint, waits for it to complete,
> then cancels the job — state is preserved. `cancel` immediately
> kills the job, discarding in-flight state since the last
> checkpoint. Use `cancel` only for "this job is broken and I don't
> care about its state". For every normal upgrade, use `stop`.

---

## Stage 2 — The savepoint trigger sequence

Triggering a savepoint is asynchronous. You get a `triggerId`, then
poll for completion:

```bash
# 1. Trigger
TRIGGER=$(curl -X POST $FLINK/jobs/$JID/savepoints \
    -d '{"target-directory":"s3://flink-savepoints","cancel-job":false}' \
    | jq -r .request-id)

# 2. Poll
while true; do
    R=$(curl -sS "$FLINK/jobs/$JID/savepoints/$TRIGGER")
    STATE=$(echo "$R" | jq -r '.status.id')
    if [ "$STATE" = "COMPLETED" ]; then
        SP=$(echo "$R" | jq -r '.operation.location')
        echo "savepoint at $SP"
        break
    fi
    sleep 2
done
```

**Bonus question:** Why is the API asynchronous?

> *Answer:* A savepoint can take seconds or minutes for jobs with
> large state. The HTTP request would time out. By returning a
> trigger ID synchronously and letting the client poll, the API
> remains responsive and works for jobs with arbitrarily large state.
> The `flink savepoint` CLI is a thin wrapper that polls for you.

---

## Stage 3 — Theory: a minimal control plane

Three responsibilities:

1. **Job lifecycle.** Track each job's intended state (running with
   image X, savepoint policy Y) and reconcile against the cluster's
   actual state. The Kubernetes Operator does this declaratively via
   CRDs.

2. **Savepoint hygiene.** A long-running job can accumulate hundreds
   of savepoints. Implement a retention policy:
   - Keep the latest 5 savepoints.
   - Keep one per week for the past 90 days.
   - Delete everything else.

   Run this as a cron-style sweeper. The savepoint paths are listed
   under `s3://flink-savepoints/<job-name>/savepoint-<id>-<sha>/`.

3. **Audit log.** "Who deployed what when, with what savepoint?".
   Store every state transition (image change, savepoint trigger,
   restore from savepoint, scale event) in your own ledger. The
   Flink JobManager's logs are not durable across restarts.

**Bonus question:** Why not just rely on the JobManager's own logs?

> *Answer:* JobManager logs live in the JM pod. When the pod restarts
> (HA failover, image upgrade), the logs go away. Even with log
> shipping (Fluent Bit, Loki), log search is line-oriented, not
> event-oriented — querying "what was the savepoint path of the last
> deploy of job X" is much easier against a structured ledger than a
> regex over logs.

---

## Stage 4 — Theory: bypassing the operator

The Kubernetes Operator handles 90% of cases. The remaining 10%:

- **Multi-region active-active.** You run the same job in two
  clusters in different regions, with cross-region savepoint
  shipping. The operator doesn't natively support this.
- **Custom autoscaling logic.** The built-in autoscaler (Lesson 16)
  uses utilization heuristics. If you have business signals (peak
  hour vs off-hour, customer-tier-specific), you build your own.
- **Atomic multi-job migrations.** "Move job A from cluster X to
  cluster Y, then start job B that consumes A's output, in one
  transaction." The operator works per-job.

For these, write a controller that uses the REST API directly. Every
operator action is a REST call; you can replicate any of them.

> *Bonus question:* Is it dangerous to run a custom controller
> alongside the Kubernetes Operator?
>
> *Answer:* Only if both touch the same FlinkDeployment. The
> operator's reconciliation loop will fight any external
> modifications and roll them back. Either pick one controller per
> deployment or have the custom controller manage REST calls to a
> session cluster (which the operator owns the JM/TM lifecycle for,
> but doesn't reconcile job-level state).

---

## When you're done

Read the upgrade script. The script is correct production-shape — you
could literally save this to `~/bin/upgrade-flink-job` and use it.

Move on to **Lesson 21 — Performance & Tuning**.
