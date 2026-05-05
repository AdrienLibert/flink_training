# Lesson 15 — Deployment

You've written the pipeline. Now you have to **run it**. Production
deployment of Flink revolves around three choices:

1. **Session mode vs application mode** — one cluster running many
   jobs vs one cluster per job.
2. **Resource sizing** — JobManager memory, TaskManager memory,
   slot count, parallelism.
3. **Where state lives** — local disk vs object store (S3, GCS, ABFS).

This lesson is mostly theory + sample configs. The Java code is a
smoke-test job you can submit to a cluster.

## Smoke test

```bash
mvn -q compile exec:exec \
    -Dexec.mainClass="com.training.flink.exercises.SmokeJob"
```

Or build the jar and submit it to a real Flink cluster:

```bash
mvn -q package
./bin/flink run -d target/deployment-job.jar
```

The jar is set up with a `Main-Class` manifest so `flink run` doesn't
need `-c`.

---

## Stage 1 — Session mode vs application mode

| Aspect | Session mode | Application mode |
| --- | --- | --- |
| Lifecycle | Cluster persists; jobs come and go | Cluster created per job, dies when job ends |
| Isolation | Shared classloader + memory | Per-job JobManager |
| User code | Runs in client JVM | Runs in JobManager JVM |
| Submit | `flink run` | `flink run-application` |
| Use for | Many small SQL queries (SQL Gateway) | One mission-critical job |
| Failure blast radius | All jobs on the cluster | Just this job |

> *Bonus question:* Why does application mode put `main()` on the
> JobManager?
>
> *Answer:* In session mode, the client JVM (the `flink run`
> invocation) compiles the job graph and sends it to the JobManager.
> The client JVM has all your dependencies on its classpath, which can
> diverge from the cluster's. In application mode, `main()` runs
> *inside* the cluster — the only classpath that matters is the one
> on the JobManager image. This is also why application mode plays
> nicely with Kubernetes: the image is the contract.

---

## Stage 2 — Sizing the JobManager

The JobManager's memory budget is partitioned by Flink:

```
process size (e.g. 1600m)
├── jvm metaspace      256m   (class loading)
├── jvm overhead       192m   (~10% of process)
├── jvm heap           ~700m  (your job graph + REST + dispatcher)
└── off-heap          ~450m  (network buffers, native libs)
```

Knobs:

- `jobmanager.memory.process.size` (preferred, total budget)
- `jobmanager.memory.heap.size` (advanced, set heap directly)

Sizing rule: 1.5–2 GB process is enough for jobs with <100 operators
and <10 concurrent SQL queries. Larger session clusters with many
running jobs need 4–8 GB.

> *Bonus question:* Why does the JobManager need its own heap if it
> doesn't process records?
>
> *Answer:* The JobManager holds the **JobGraph**, the **ExecutionGraph**
> with one vertex per task subtask (so a 1000-parallelism job has
> 1000 entries per operator), the **dispatcher** with REST handlers
> for every running and historical job, and the **CheckpointCoordinator**
> tracking pending checkpoints. For a job with 10k subtasks, the
> ExecutionGraph alone is hundreds of MB.

---

## Stage 3 — Sizing the TaskManager

The TaskManager is where records flow:

```
process size (e.g. 4096m)
├── jvm metaspace        256m
├── jvm overhead         410m   (~10%)
├── framework heap        128m   (Flink internal)
├── framework off-heap    128m
├── task heap            ~1500m (your operators' state + buffers)
├── task off-heap          0m   (set if you use direct memory)
├── managed memory       ~1100m (RocksDB, sort buffers)
└── network              ~400m  (network buffers between subtasks)
```

The crucial knob is `taskmanager.numberOfTaskSlots`. **Each slot is a
fraction of the TM's resources.** Slots = 4 means each slot gets ¼ of
the TM's CPU + memory. The rule: slots ≈ vCPU cores per TM, with one
TM per Kubernetes pod (one CPU set per JVM, simpler bin-packing).

> *Bonus question:* Why prefer many small TMs over few large ones?
>
> *Answer:* Small TMs (4-8 slots, 4-16 GB) fail and recover
> independently. A single 32-slot TM crash takes 32 subtasks down at
> once, all needing redistribution. Smaller TMs also pack better in a
> Kubernetes scheduler that has heterogeneous nodes. The downside is
> more JVMs (more metaspace overhead in aggregate); for production,
> the failure-isolation argument usually wins.

---

## Stage 4 — Sample `flink-conf.yaml`

See `configs/flink-conf.yaml` for a production-shaped config. Read
it line by line — every value is a deliberate choice:

- `state.backend: rocksdb` — for any job with significant keyed
  state. Default `hashmap` only fits small state in heap.
- `state.backend.incremental: true` — RocksDB only writes the
  *changed* SST files since the last checkpoint, dramatically faster.
- `execution.checkpointing.unaligned: true` — alignment is the
  major cause of slow checkpoints under backpressure; unaligned
  trades a small state-size hit for much shorter checkpoint duration.
- `high-availability.type: kubernetes` — uses a Kubernetes ConfigMap
  for leader election. Replaces the old ZooKeeper-based HA.

---

## Stage 5 — Submitting a job

Three production-shaped invocations:

```bash
# 1. Session-mode submission to an existing cluster
./bin/flink run -d \
    -p 8 \
    -s s3://flink-savepoints/my-job/savepoint-XXXX \
    target/deployment-job.jar

# 2. Application-mode on Kubernetes
./bin/flink run-application -t kubernetes-application \
    -Dkubernetes.cluster-id=my-cluster \
    -Dkubernetes.container.image=registry/my-job:abc123 \
    -Dkubernetes.namespace=streaming \
    local:///opt/flink/usrlib/deployment-job.jar

# 3. Stop with savepoint (graceful)
./bin/flink stop --savepointPath s3://flink-savepoints/my-job <jobid>
```

Notes:
- `-d` = detached (don't tail logs in the client).
- `-s` = restore from a savepoint.
- `local:///` is required in application mode (the path must exist
  in the JobManager image, NOT on the client filesystem).
- `flink stop` with savepoint is the **only** safe way to upgrade —
  `flink cancel` discards in-flight state.

---

## When you're done

Read `configs/flink-conf.yaml`. Move on to **Lesson 16 — Flink
Kubernetes Operator**.
