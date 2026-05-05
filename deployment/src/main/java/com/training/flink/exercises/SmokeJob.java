package com.training.flink.exercises;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Lesson 15 — Deployment.
 *
 * A trivial pipeline used as a deployment smoke test. Build the jar
 * with `mvn package` and submit it to a Flink cluster:
 *
 *   ./bin/flink run -d target/deployment-job.jar
 *
 * For the application-mode equivalent (job submitted to its own
 * JobManager, more isolated):
 *
 *   ./bin/flink run-application -t kubernetes-application \
 *      -Dkubernetes.cluster-id=my-cluster \
 *      -Dkubernetes.container.image=flink:1.18.1 \
 *      local:///opt/flink/usrlib/deployment-job.jar
 *
 * Run it locally too:
 *
 *   mvn -q compile exec:exec \
 *       -Dexec.mainClass="com.training.flink.exercises.SmokeJob"
 */
public class SmokeJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1, 2, 3, 4, 5)
                .map(x -> "deployed-" + x)
                .print();

        env.execute("deployment-smoke");
    }
}
