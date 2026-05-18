package com.onlinejudge.scoring.smoke;

import com.onlinejudge.common.events.Events.VerdictEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Produces a deterministic 2-user, 2-problem ICPC scenario plus a watermark
 * sentinel to the regional Kafka topic the smoke test consumes from after
 * MirrorMaker 2 mirrors it onto kafka-global as {@code regional.evaluated_results}.
 *
 * <p>Driven by {@code scripts/scoring-smoke.sh}. Inputs are env-only so the
 * Gradle {@code runSmokeProducer} task and the shell driver stay decoupled.
 */
public final class SmokeProducer {

    private static final String SENTINEL_USER    = "sentinel-user";
    private static final String SENTINEL_PROBLEM = "sentinel-problem";

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic     = env("SMOKE_TOPIC",     "evaluated_results");
        String contestId = required("SMOKE_CONTEST");
        long   baseTsMs  = Long.parseLong(required("SMOKE_BASE_TS_MS"));

        long tACUserAP1     = baseTsMs;
        long tWAUserAP2     = baseTsMs + TimeUnit.MINUTES.toMillis(10);
        long tACUserAP2     = baseTsMs + TimeUnit.MINUTES.toMillis(15);
        long tACUserBP1     = baseTsMs;
        long tSentinelEvent = baseTsMs + TimeUnit.MINUTES.toMillis(21);

        System.out.printf("[smoke-producer] bootstrap=%s topic=%s contest=%s base=%d%n",
                bootstrap, topic, contestId, baseTsMs);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "scoring-smoke-producer");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);

        try (Producer<String, byte[]> producer = new KafkaProducer<>(props)) {

            send(producer, topic, "userA",
                    verdict("sub-A-P1", "userA", "P1", contestId, "ACCEPTED", 100, tACUserAP1));
            send(producer, topic, "userA",
                    verdict("sub-A-P2-wa", "userA", "P2", contestId, "WRONG_ANSWER", 0, tWAUserAP2));
            send(producer, topic, "userA",
                    verdict("sub-A-P2-ac", "userA", "P2", contestId, "ACCEPTED", 100, tACUserAP2));
            send(producer, topic, "userB",
                    verdict("sub-B-P1", "userB", "P1", contestId, "ACCEPTED", 100, tACUserBP1));

            // Watermark sentinel: emit one ACCEPTED for a non-asserted user/problem to
            // EVERY partition at a timestamp comfortably past the latest real event +
            // BoundedOutOfOrderness(5min). This advances the per-partition watermark
            // even on partitions that received no real verdicts, so the ScoringFunction
            // is not waiting on an idle partition. The sentinel's contestId is also
            // distinct from SMOKE_CONTEST so it cannot perturb assertions on the run.
            String sentinelContest = contestId + "::sentinel";
            List<PartitionInfo> partitions = producer.partitionsFor(topic);
            System.out.printf("[smoke-producer] emitting watermark sentinel to %d partition(s)%n",
                    partitions.size());
            for (PartitionInfo p : partitions) {
                VerdictEvent sentinel = verdict(
                        "sub-sentinel-p" + p.partition(),
                        SENTINEL_USER, SENTINEL_PROBLEM, sentinelContest,
                        "ACCEPTED", 1, tSentinelEvent);
                producer.send(new ProducerRecord<>(
                        topic, p.partition(), SENTINEL_USER, sentinel.toByteArray()));
            }

            producer.flush();
            System.out.println("[smoke-producer] all sends acknowledged");
        }
    }

    private static void send(Producer<String, byte[]> producer,
                             String topic, String key, VerdictEvent event) {
        producer.send(new ProducerRecord<>(topic, key, event.toByteArray()));
    }

    private static VerdictEvent verdict(String submissionId,
                                        String userId,
                                        String problemId,
                                        String contestId,
                                        String result,
                                        int points,
                                        long eventTsMs) {
        return VerdictEvent.newBuilder()
                .setSubmissionId(submissionId)
                .setUserId(userId)
                .setProblemId(problemId)
                .setContestId(contestId)
                .setResult(result)
                .setPoints(points)
                .setPhase("system")
                .setRegion("smoke")
                .setGatewayTsMs(eventTsMs)
                .setEventTsMs(eventTsMs)
                .build();
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    private static String required(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("required env var missing: " + key);
        }
        return v;
    }

    private SmokeProducer() {}
}
