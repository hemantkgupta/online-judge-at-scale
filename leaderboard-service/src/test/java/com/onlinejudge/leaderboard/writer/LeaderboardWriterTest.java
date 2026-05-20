package com.onlinejudge.leaderboard.writer;

import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the stand-in {@link LeaderboardWriter}.
 *
 * Verifies:
 * <ul>
 *   <li>Lua KEYS shape: [pubsubChannel, targetShardKey, ...otherShardKeys] — same as
 *       scoring-pipeline's {@code RedisLeaderboardSink} so the two writers
 *       agree during cutover.</li>
 *   <li>Lua ARGV shape: [userId, zsetScore, contestId, totalPoints, "0"] —
 *       penalty is always "0" (the stand-in deliberately doesn't do ICPC
 *       penalty math; that's Flink's job).</li>
 *   <li>ACCEPTED + {@code phase=system} triggers a write; pretest is skipped;
 *       non-ACCEPTED verdicts are skipped.</li>
 *   <li>Same submission id arriving twice is a no-op (SADD-based dedupe).</li>
 *   <li>Missing user / contest / submission ids are dropped.</li>
 *   <li>{@code phase=final} is treated the same as {@code phase=system}.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class LeaderboardWriterTest {

    @Mock private StringRedisTemplate redisTemplate;
    @Mock private SetOperations<String, String> setOps;
    @Mock private HashOperations<String, Object, Object> hashOps;

    private final ScoreRangeShardRouter router = ScoreRangeShardRouter.defaultIcpcRouter();
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private LeaderboardWriter writer;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setUp() {
        writer = new LeaderboardWriter(redisTemplate, router, meterRegistry);
        // Make Redis ops navigable. `lenient` so tests that filter early
        // (and never touch ops) don't fail UnnecessaryStubbingException.
        lenient().when(redisTemplate.opsForSet()).thenReturn(setOps);
        lenient().when(redisTemplate.opsForHash()).thenReturn((HashOperations) hashOps);
        // Default: this submission has not been processed before (SADD returns 1).
        lenient().when(setOps.add(anyString(), anyString())).thenReturn(1L);
    }

    private static VerdictEvent.Builder verdict() {
        return VerdictEvent.newBuilder()
                .setSubmissionId("sub-1")
                .setUserId("user-1")
                .setContestId("c-1")
                .setProblemId("p-1")
                .setResult("ACCEPTED")
                .setPhase("system")
                .setPoints(100);
    }

    @Test
    @SuppressWarnings("unchecked")
    void onVerdict_acceptedSystemPhase_writesLuaWithCorrectKeysAndArgs() {
        // 100 points after this submission; shard router on default ICPC
        // boundaries [0, 30M, 60M] puts 100 * 10M = 1_000_000_000 in shard 2.
        when(hashOps.increment(eq("leaderboard:state:c-1"), eq("user-1"), eq(100L))).thenReturn(100L);

        boolean wrote = writer.onVerdict(verdict().build());

        assertThat(wrote).isTrue();

        // Capture the eval call so we can assert KEYS / ARGV shape.
        ArgumentCaptor<RedisScript<Long>> scriptCap = ArgumentCaptor.forClass(RedisScript.class);
        ArgumentCaptor<List<String>> keysCap = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Object[]> argsCap = ArgumentCaptor.forClass(Object[].class);
        verify(redisTemplate).execute(scriptCap.capture(), keysCap.capture(), argsCap.capture());

        // KEYS: [pubsubChannel, targetShard, ...otherShards] — exactly the
        // shape RedisLeaderboardSink uses, so a brief writer overlap during
        // cutover is no-op.
        List<String> keys = keysCap.getValue();
        assertThat(keys).hasSize(router.shardCount() + 1);
        assertThat(keys.get(0)).isEqualTo("score_updates:c-1");
        // 100 * 10M = 1B → shard 2 on the default ICPC boundaries.
        assertThat(keys.get(1)).isEqualTo("leaderboard:c-1:shard:2");
        // Remaining keys are the OTHER shards (order: 0, 1 — i.e. ascending,
        // skipping the target).
        assertThat(keys.subList(2, keys.size()))
                .containsExactly("leaderboard:c-1:shard:0", "leaderboard:c-1:shard:1");

        // ARGV: [userId, zsetScore, contestId, totalPoints, "0"]
        Object[] args = argsCap.getValue();
        assertThat(args).hasSize(5);
        assertThat(args[0]).isEqualTo("user-1");
        assertThat(Double.parseDouble((String) args[1])).isEqualTo(1_000_000_000.0); // 100 * 10M
        assertThat(args[2]).isEqualTo("c-1");
        assertThat(args[3]).isEqualTo("100"); // newTotal
        assertThat(args[4]).isEqualTo("0");   // penalty = 0 in stand-in

        // Counter incremented exactly once.
        double count = meterRegistry.find("oj.leaderboard.writer.writes_total")
                .tag("contest_id", "c-1")
                .tag("verdict", "ACCEPTED")
                .counter()
                .count();
        assertThat(count).isEqualTo(1.0);

        // Dedupe set TTL is refreshed on every successful add.
        verify(redisTemplate).expire(eq("processed:c-1"), eq(Duration.ofHours(24)));
    }

    @Test
    void onVerdict_acceptedFinalPhase_alsoWrites() {
        when(hashOps.increment(anyString(), any(), anyLong())).thenReturn(50L);

        boolean wrote = writer.onVerdict(verdict().setPhase("final").setPoints(50).build());

        assertThat(wrote).isTrue();
        verify(redisTemplate).execute(any(RedisScript.class), any(List.class), any(Object[].class));
    }

    @Test
    void onVerdict_pretestPhase_isSkipped() {
        boolean wrote = writer.onVerdict(verdict().setPhase("pretest").build());

        assertThat(wrote).isFalse();
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(List.class), any(Object[].class));
        // We never even consulted the dedupe set.
        verify(setOps, never()).add(anyString(), anyString());
    }

    @Test
    void onVerdict_wrongAnswer_isSkipped() {
        boolean wrote = writer.onVerdict(verdict().setResult("WRONG_ANSWER").build());

        assertThat(wrote).isFalse();
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(List.class), any(Object[].class));
    }

    @Test
    void onVerdict_missingUserId_isSkipped() {
        boolean wrote = writer.onVerdict(verdict().clearUserId().build());

        assertThat(wrote).isFalse();
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(List.class), any(Object[].class));
    }

    @Test
    void onVerdict_missingContestId_isSkipped() {
        boolean wrote = writer.onVerdict(verdict().clearContestId().build());

        assertThat(wrote).isFalse();
    }

    @Test
    void onVerdict_missingSubmissionId_isSkipped() {
        boolean wrote = writer.onVerdict(verdict().clearSubmissionId().build());

        assertThat(wrote).isFalse();
    }

    @Test
    void onVerdict_zeroPoints_isSkipped() {
        // ACCEPTED on a 0-point problem can't move the ZSET; skip the round-trip.
        boolean wrote = writer.onVerdict(verdict().setPoints(0).build());

        assertThat(wrote).isFalse();
        verify(redisTemplate, never()).execute(any(RedisScript.class), any(List.class), any(Object[].class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void onVerdict_sameSubmissionTwice_writesOnceOnly() {
        when(hashOps.increment(anyString(), any(), anyLong())).thenReturn(100L);
        // First call: SADD returns 1 (new).
        // Second call: SADD returns 0 (already-present) → must skip.
        when(setOps.add(eq("processed:c-1"), eq("sub-1")))
                .thenReturn(1L)
                .thenReturn(0L);

        VerdictEvent v = verdict().build();
        assertThat(writer.onVerdict(v)).isTrue();
        assertThat(writer.onVerdict(v)).isFalse();

        // execute() ran exactly once.
        verify(redisTemplate, times(1))
                .execute(any(RedisScript.class), any(List.class), any(Object[].class));
        // INCRBY also ran exactly once — duplicates must not double-count points.
        verify(hashOps, times(1)).increment(anyString(), any(), anyLong());
    }

    @Test
    void encode_mirrorsScoreEncoder() {
        // ScoreEncoder.encode(300, 45) = 2_999_999_955.0
        assertThat(LeaderboardWriter.encode(300L, 45L)).isEqualTo(2_999_999_955.0);
        // Stand-in always uses penalty = 0.
        assertThat(LeaderboardWriter.encode(100L, 0L)).isEqualTo(1_000_000_000.0);
        assertThat(LeaderboardWriter.encode(0L, 0L)).isEqualTo(0.0);
    }
}
