package com.onlinejudge.worker.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.worker.service.DockerExecutionService;
import com.onlinejudge.worker.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Execution worker: polls the T1 (pretest) Kafka topic.
 *
 * Pull model: the worker polls Kafka only when it has an execution slot free
 * (max-poll-records=1 ensures one submission per consumer thread).
 * When a worker is busy executing, it stops polling -- backpressure is automatic.
 *
 * Exactly-once via idempotency check before executing.
 * Kafka offset is committed only AFTER the verdict is published.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubmissionConsumer {

    private final IdempotencyService idempotencyService;
    private final DockerExecutionService executionService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.evaluated-results}")
    private String evaluatedResultsTopic;

    @Value("${app.kafka.topic.analytics}")
    private String analyticsTopic;

    @KafkaListener(
        topics = "${app.kafka.topic.pretest}",
        groupId = "execution-worker-t1",
        concurrency = "4"   // 4 concurrent consumer threads = 4 parallel executions
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String submissionId = null;
        try {
            JsonNode event = objectMapper.readTree(record.value());
            submissionId = event.get("submissionId").asText();
            String userId = event.get("userId").asText();
            String problemId = event.get("problemId").asText();
            String contestId = event.has("contestId") && !event.get("contestId").isNull()
                    ? event.get("contestId").asText() : null;
            String language = event.get("language").asText();
            long gatewayTsMs = event.get("gatewayTsMs").asLong();

            log.info("[worker] Received submission={} user={} lang={}", submissionId, userId, language);

            // Idempotency check: skip if already processed (Kafka at-least-once redelivery)
            if (!idempotencyService.claimSubmission(submissionId)) {
                ack.acknowledge();
                return;
            }

            // Execute code in Docker container
            // TODO: fetch actual code from S3/R2; use sample code for local dev
            String sampleCode = getSampleCode(language);
            String sampleInput = "5\n3 1 4 1 5";
            DockerExecutionService.ExecutionResult result =
                    executionService.execute(submissionId, language, sampleCode, sampleInput);

            // Determine verdict
            String verdict = determineVerdict(result, 2000);
            int points = verdict.equals("ACCEPTED") ? 100 : 0;

            log.info("[worker] Verdict submission={} result={} time={}ms",
                    submissionId, verdict, result.executionTimeMs());

            // Publish verdict to evaluated_results topic (keyed by userId for Flink ordering)
            Map<String, Object> verdictPayload = new HashMap<>();
            verdictPayload.put("submissionId", submissionId);
            verdictPayload.put("userId", userId);
            verdictPayload.put("problemId", problemId);
            verdictPayload.put("contestId", contestId);
            verdictPayload.put("result", verdict);
            verdictPayload.put("executionTimeMs", result.executionTimeMs());
            verdictPayload.put("memoryUsedMb", result.memoryUsedMb());
            verdictPayload.put("gatewayTsMs", gatewayTsMs);
            verdictPayload.put("points", points);

            byte[] verdictBytes = objectMapper.writeValueAsBytes(verdictPayload);
            kafkaTemplate.send(evaluatedResultsTopic, userId, verdictBytes);

            // Publish to analytics topic (fire-and-forget, separate consumer group)
            Map<String, Object> analyticsPayload = new HashMap<>();
            analyticsPayload.put("submissionId", submissionId);
            analyticsPayload.put("userId", userId);
            analyticsPayload.put("problemId", problemId);
            analyticsPayload.put("contestId", contestId);
            analyticsPayload.put("language", language);
            analyticsPayload.put("verdict", verdict);
            analyticsPayload.put("executionTimeMs", result.executionTimeMs());
            analyticsPayload.put("memoryUsedMb", result.memoryUsedMb());
            analyticsPayload.put("eventTsMs", System.currentTimeMillis());

            kafkaTemplate.send(analyticsTopic, submissionId, objectMapper.writeValueAsBytes(analyticsPayload));

            // Mark idempotency key as completed
            idempotencyService.markCompleted(submissionId);

            // Commit Kafka offset ONLY after verdict is published
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[worker] Error processing submission={}: {}", submissionId, ex.getMessage(), ex);
            // Do NOT ack - Kafka will redeliver. Idempotency key prevents double execution.
        }
    }

    private String determineVerdict(DockerExecutionService.ExecutionResult result, int timeLimitMs) {
        return switch (result.status()) {
            case "TIME_LIMIT_EXCEEDED" -> "TIME_LIMIT_EXCEEDED";
            case "RUNTIME_ERROR"       -> "RUNTIME_ERROR";
            case "INTERNAL_ERROR"      -> "RUNTIME_ERROR";
            case "OK" -> result.executionTimeMs() > timeLimitMs
                    ? "TIME_LIMIT_EXCEEDED" : "ACCEPTED";
            default -> "WRONG_ANSWER";
        };
    }

    // Placeholder: real worker fetches code from S3/R2 using s3CodeUrl from event
    private String getSampleCode(String language) {
        return switch (language) {
            case "python" -> "import sys\ndata = sys.stdin.read().split()\nprint(sum(int(x) for x in data[1:]))";
            case "java"   -> "import java.util.*; public class Solution { public static void main(String[] a) { Scanner sc = new Scanner(System.in); int n=sc.nextInt(); long s=0; for(int i=0;i<n;i++) s+=sc.nextInt(); System.out.println(s); } }";
            default       -> "print(42)";
        };
    }
}
