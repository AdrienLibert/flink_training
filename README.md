# Flink Training

A hands-on training to deeply understand Apache Flink — pipeline internals,
state management, deployment, and production tuning.

Each lesson lives on its **own branch** so you can check out one topic at a
time without intermediate code from later lessons polluting your view.

## Repository layout

| Branch | Contents |
|--------|----------|
| `main` | This README + `courses.md` curriculum overview |
| `lesson_1_datastream_api` | Lesson 1 — Mastering DataStream API (5 exercises + reference solutions) |
| _(more branches as lessons are added)_ | |

## How to use this repo

1. Read [`courses.md`](./courses.md) for the full curriculum.
2. Check out the branch for the lesson you want:
   ```bash
   git checkout lesson_1_datastream_api
   ```
3. Each lesson folder contains:
   - A Maven project (`pom.xml`)
   - `src/main/java/.../exercises/` — starter files with TODOs
   - `src/main/java/.../solutions/` — full reference solutions with bonus-question answers in comments
   - A README or EXERCISES.md with goals and run instructions

## Prerequisites

- **Java 11+**
- **Maven 3.6+**
- **Apache Flink 1.18+** (pulled in via Maven, no separate install needed)

Helpful but not required:
- Familiarity with distributed systems concepts
- 1+ year working with Flink or similar streaming systems
- Basic Kubernetes knowledge (used in later production lessons)

## Curriculum at a glance

- **Flink Core APIs** — DataStream, Table & SQL, Connectors
- **Architecting Efficient Pipelines** — dataflow design, enrichment, skew, batch
- **Flink in Production** — deployment, reliability, observability, tuning, K8s

See [`courses.md`](./courses.md) for the full lesson-by-lesson breakdown.

## Running an exercise

Inside any lesson folder:

```bash
mvn -q compile exec:exec -Dexec.mainClass="com.training.flink.exercises.Exercise1_BasicPipeline"
```

Replace the main class path to run a different exercise or its solution
(under `com.training.flink.solutions...`).
