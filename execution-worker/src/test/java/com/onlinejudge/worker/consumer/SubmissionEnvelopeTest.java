package com.onlinejudge.worker.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.common.events.Events.SubmissionEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SubmissionConsumer#parseSubmissionEvent} — the small adapter that
 * lets a single consumer code path serve both producers of submission events:
 *
 * <ul>
 *   <li>The polling outbox publisher (proto bytes — default, Part 2 wire format).</li>
 *   <li>The CockroachDB native changefeed (wrapped JSON envelope, opt-in CDC path).</li>
 *   <li>A bare JSON payload (legacy form, retained for the smoke test and any tooling
 *       that prefers JSON over the wire).</li>
 * </ul>
 *
 * <p>The parser is invoked once per Kafka record before any business logic runs;
 * it must be cheap, defensive, and side-effect-free.
 */
class SubmissionEnvelopeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SubmissionConsumer consumer = new SubmissionConsumer(null, null, null, objectMapper);

    @Test
    void protoBytes_areParsedDirectly() throws Exception {
        // The default api-gateway → Kafka wire format: a proto SubmissionEvent.
        SubmissionEvent event = SubmissionEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setUserId("u-1")
                .setProblemId("p-1")
                .setLanguage("python")
                .setGatewayTsMs(12345)
                .setRegion("us-east-1")
                .setPhase("pretest")
                .build();

        SubmissionEvent decoded = consumer.parseSubmissionEvent(event.toByteArray());

        assertThat(decoded.getSubmissionId()).isEqualTo("sub-1");
        assertThat(decoded.getUserId()).isEqualTo("u-1");
        assertThat(decoded.getRegion()).isEqualTo("us-east-1");
        assertThat(decoded.getGatewayTsMs()).isEqualTo(12345L);
    }

    @Test
    void cockroachChangefeedEnvelope_isTranscodedToProto() throws Exception {
        // CockroachDB changefeed `envelope=wrapped` shape: the outbox row's `payload`
        // column (itself a JSON string) is nested inside the `after` object.
        String innerPayload = "{\"submissionId\":\"sub-cdc-1\",\"userId\":\"u-cdc\"," +
                "\"problemId\":\"p\",\"language\":\"python\",\"gatewayTsMs\":98765," +
                "\"region\":\"eu-west-1\"}";
        String envelopeJson = "{\"after\":{" +
                "\"id\":\"00000000-0000-0000-0000-000000000001\"," +
                "\"submission_id\":\"sub-cdc-1\"," +
                "\"event_type\":\"SUBMISSION_RECEIVED\"," +
                "\"payload\":" + objectMapper.writeValueAsString(innerPayload) + "," +
                "\"published\":false," +
                "\"created_at\":\"2026-05-13T00:00:00Z\"" +
                "},\"updated\":\"1715570400000000000.0000000000\"}";

        SubmissionEvent decoded = consumer.parseSubmissionEvent(envelopeJson.getBytes());

        assertThat(decoded.getSubmissionId()).isEqualTo("sub-cdc-1");
        assertThat(decoded.getUserId()).isEqualTo("u-cdc");
        assertThat(decoded.getGatewayTsMs()).isEqualTo(98765L);
        assertThat(decoded.getRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void bareJsonPayload_isAlsoAccepted() throws Exception {
        String raw = "{\"submissionId\":\"sub-bare\",\"userId\":\"u-bare\",\"problemId\":\"p-bare\"," +
                "\"language\":\"python\",\"gatewayTsMs\":42,\"region\":\"ap-south-1\"}";

        SubmissionEvent decoded = consumer.parseSubmissionEvent(raw.getBytes());

        assertThat(decoded.getSubmissionId()).isEqualTo("sub-bare");
        assertThat(decoded.getUserId()).isEqualTo("u-bare");
        assertThat(decoded.getRegion()).isEqualTo("ap-south-1");
    }

    @Test
    void dataUrlSourceCode_isDecoded() {
        String source = "print(42)\n";
        String dataUrl = "data:text/plain;charset=utf-8;base64," +
                Base64.getEncoder().encodeToString(source.getBytes(StandardCharsets.UTF_8));

        assertThat(consumer.resolveSourceCode(dataUrl)).isEqualTo(source);
    }

    @Test
    void localUrlWithoutEmbeddedSource_isRejected() {
        assertThatThrownBy(() -> consumer.resolveSourceCode("local://submissions/1/code"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("data: URLs");
    }
}
