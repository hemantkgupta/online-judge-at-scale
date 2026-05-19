package com.onlinejudge.leaderboard.writer;

import com.onlinejudge.common.events.Events.VerdictEvent;
import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Stand-in leaderboard writer: per-verdict ZADD to the score-range ZSETs.
 *
 * <p><b>What this is.</b> A simple ZADD-by-points writer that lives inside
 * leaderboard-service and runs off the same {@code @KafkaListener} that powers
 * the verdict-cache / WebSocket push. It exists so M1 of the tech-spec roadmap
 * (§14) can close <em>with working leaderboard writes</em> before the
 * scoring-pipeline Flink job is productionised. When scoring-pipeline is up,
 * the operator flips {@code app.leaderboard.writer.enabled=false} and Flink
 * becomes the sole writer; this bean drops out of the context, the
 * {@code @KafkaListener} keeps running, and the read path / WebSocket fan-out
 * are unaffected.
 *
 * <p><b>Scope.</b> Penalty = 0. Proper ICPC penalty math (first-AC-wins,
 * pre-AC wrong attempts × 20 min) lives in scoring-pipeline's
 * {@code Scorer}; doing it here would mean replicating the keyed state
 * machine and we'd argue forever about whose math wins during the cutover.
 * The stand-in is documented as "simple ZADD-by-points" — that's all it does.
 * Points come straight off the {@link VerdictEvent#getPoints()} field that
 * execution-worker stamps on the verdict (proto field 9).
 *
 * <p><b>Phase filter.</b> Only ACCEPTED verdicts with {@code phase} in
 * {@code {system, final}} are scored — same predicate as
 * scoring-pipeline's {@code Scorer.isScoreablePhase}. Pretest results are
 * provisional and must not become final score if the later system-test fails.
 *
 * <p><b>Shard routing.</b> {@link ScoreRangeShardRouter#defaultIcpcRouter()}
 * — the same router instance the scoring-pipeline sink uses. Cutover never
 * moves a user between shards because both writers compute the shard the
 * same way from the same composite score.
 *
 * <p><b>Atomicity.</b> A single Lua script (mirrors the body of
 * {@code RedisLeaderboardSink.LUA_UPDATE_SCORE} in scoring-pipeline):
 * <ol>
 *   <li>{@code ZREM} the user from every non-target shard.</li>
 *   <li>{@code ZADD} the user to the target shard with the new composite score.</li>
 *   <li>{@code PUBLISH} the score-change event on {@code score_updates:{contestId}}
 *       for the WebSocket differential push.</li>
 * </ol>
 * Identical key + arg shape on both sides — brief overlap during cutover is a
 * no-op in score-encoding space because both writers compute the same composite
 * float from the same {@code (totalPoints, 0)} pair on the same user, and ZADD
 * is idempotent at equal score.
 *
 * <p><b>Idempotency across Kafka redeliveries.</b> The Kafka consumer is
 * {@code auto-commit=true} and {@code auto-offset-reset=latest}, but redelivery
 * within a poll window can still happen (rebalance, retry). The stand-in
 * tracks processed submission ids in a per-contest Redis set
 * {@code processed:{contestId}} with a 24 h TTL — same horizon as the
 * verdict-cache. {@code SADD} returns 0 if the submission id is already
 * present, in which case the Lua run is skipped. The TTL gets refreshed on
 * every successful write so an active contest never expires mid-flight.
 *
 * <p><b>Metric.</b> {@code oj.leaderboard.writer.writes_total} (Counter,
 * labels {@code contest_id}, {@code verdict}). The operator dashboard tile
 * compares this rate against {@code oj.scoring.score_updates_total} from
 * Flink — when both rates match for N contests, the flip is safe.
 *
 * <p><b>Composite score formula</b> (mirrors {@code ScoreEncoder.encode}):
 * {@code (long) totalPoints * 10_000_000L - penaltyMinutes}.
 * For the stand-in {@code penaltyMinutes} is always 0, so it reduces to
 * {@code totalPoints * 10_000_000L}.
 */
@Slf4j
@Component
@ConditionalOnProperty(
        name = "app.leaderboard.writer.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class LeaderboardWriter {

    /**
     * Atomic Lua. KEYS / ARGV ordering MUST match
     * {@code RedisLeaderboardSink.LUA_UPDATE_SCORE} in scoring-pipeline.
     *
     * KEYS[1]    = Pub/Sub channel ({@code score_updates:{contestId}})
     * KEYS[2]    = target shard key
     * KEYS[3..N] = other shard keys to ZREM the user from (boundary-cross cleanup)
     *
     * ARGV[1] = user_id
     * ARGV[2] = new composite zsetScore (Lua tonumber)
     * ARGV[3] = contest_id
     * ARGV[4] = totalPoints  (echoed into the Pub/Sub message body)
     * ARGV[5] = penaltyMinutes (echoed; always 0 in the stand-in)
     */
    static final String LUA_UPDATE_SCORE =
            "local pubsub_channel   = KEYS[1]\n" +
            "local target_shard_key = KEYS[2]\n" +
            "local user_id          = ARGV[1]\n" +
            "local new_score        = tonumber(ARGV[2])\n" +
            "local contest_id       = ARGV[3]\n" +
            "local total_score      = ARGV[4]\n" +
            "local penalty          = ARGV[5]\n" +
            "\n" +
            "for i = 3, #KEYS do\n" +
            "    redis.call('ZREM', KEYS[i], user_id)\n" +
            "end\n" +
            "\n" +
            "redis.call('ZADD', target_shard_key, new_score, user_id)\n" +
            "local new_rank = redis.call('ZREVRANK', target_shard_key, user_id)\n" +
            "\n" +
            "local msg = '{\"userId\":\"' .. user_id\n" +
            "          .. '\",\"contestId\":\"' .. contest_id\n" +
            "          .. '\",\"newScore\":' .. total_score\n" +
            "          .. ',\"penaltyMinutes\":' .. penalty\n" +
            "          .. ',\"newRank\":' .. (new_rank or 0)\n" +
            "          .. '}'\n" +
            "redis.call('PUBLISH', pubsub_channel, msg)\n" +
            "return new_rank\n";

    /** ZSET-score encoding multiplier — must agree with {@code ScoreEncoder.SCORE_MULTIPLIER}. */
    static final long SCORE_MULTIPLIER = 10_000_000L;

    /** Scoreable phases — must agree with scoring-pipeline's {@code Scorer.SCOREABLE_PHASES}. */
    private static final Set<String> SCOREABLE_PHASES = Set.of("system", "final");

    /** Idempotency-key TTL: matches the {@code verdict:{id}} cache TTL. */
    static final Duration PROCESSED_TTL = Duration.ofHours(24);

    /** Per-contest user-total points hash. Survives across submissions in the contest. */
    static final String STATE_KEY_PREFIX = "leaderboard:state:";

    /** Per-contest processed-submission set for at-least-once-aware idempotency. */
    static final String PROCESSED_KEY_PREFIX = "processed:";

    private final StringRedisTemplate redisTemplate;
    private final ScoreRangeShardRouter shardRouter;
    private final MeterRegistry meterRegistry;
    private final DefaultRedisScript<Long> script;

    public LeaderboardWriter(StringRedisTemplate redisTemplate,
                             ScoreRangeShardRouter shardRouter,
                             MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.shardRouter = shardRouter;
        this.meterRegistry = meterRegistry;
        this.script = new DefaultRedisScript<>(LUA_UPDATE_SCORE, Long.class);
    }

    /**
     * Apply one verdict to the leaderboard, if it is scoreable.
     *
     * @return {@code true} if a write actually happened, {@code false} if the
     *         verdict was filtered (non-ACCEPTED, non-scoreable phase, missing
     *         ids, zero points, duplicate submission).
     */
    public boolean onVerdict(VerdictEvent verdict) {
        if (!isScoreable(verdict)) {
            return false;
        }
        String userId       = verdict.getUserId();
        String contestId    = verdict.getContestId();
        String submissionId = verdict.getSubmissionId();
        int points          = verdict.getPoints();

        // Idempotency: skip if we've already scored this submission.
        // SADD returns 1 on insert, 0 on already-present.
        String processedKey = PROCESSED_KEY_PREFIX + contestId;
        Long added = redisTemplate.opsForSet().add(processedKey, submissionId);
        if (added == null || added == 0L) {
            log.debug("[lb-writer] duplicate submission, skipping. contestId={} submissionId={}",
                    contestId, submissionId);
            return false;
        }
        // Refresh the TTL on every successful add so a 24h contest doesn't lose
        // its dedupe set mid-flight. Cheap (EXPIRE is O(1)).
        redisTemplate.expire(processedKey, PROCESSED_TTL);

        // Accumulate total points for this user across the contest. INCRBY is
        // atomic; the returned value is the new total.
        String stateKey = STATE_KEY_PREFIX + contestId;
        Long newTotalPointsBoxed = redisTemplate.opsForHash().increment(stateKey, userId, points);
        long newTotalPoints = newTotalPointsBoxed != null ? newTotalPointsBoxed : points;
        // Bound state-key TTL the same way; refreshed on every write.
        redisTemplate.expire(stateKey, PROCESSED_TTL);

        // Stand-in penalty: 0. Proper penalty math is scoring-pipeline's job.
        double zsetScore = encode(newTotalPoints, 0);
        int targetShard = shardRouter.shardForScore(zsetScore);

        // KEYS = [pubsubChannel, targetShard, ...otherShards] — same shape as
        // RedisLeaderboardSink so the Lua bodies are interchangeable.
        List<String> keys = new ArrayList<>(shardRouter.shardCount() + 1);
        keys.add("score_updates:" + contestId);
        keys.add(shardRouter.shardKey(contestId, targetShard));
        for (int i = 0; i < shardRouter.shardCount(); i++) {
            if (i != targetShard) {
                keys.add(shardRouter.shardKey(contestId, i));
            }
        }

        redisTemplate.execute(
                script,
                keys,
                userId,
                Double.toString(zsetScore),
                contestId,
                Long.toString(newTotalPoints),
                "0"
        );

        Counter.builder("oj.leaderboard.writer.writes_total")
                .description("Stand-in leaderboard writes. Compare against oj.scoring.score_updates_total before cutover.")
                .tag("contest_id", contestId)
                .tag("verdict", verdict.getResult())
                .register(meterRegistry)
                .increment();

        log.debug("[lb-writer] wrote user={} contest={} shard={} total={} zsetScore={}",
                userId, contestId, targetShard, newTotalPoints, zsetScore);
        return true;
    }

    private static boolean isScoreable(VerdictEvent v) {
        if (v.getUserId().isEmpty() || v.getContestId().isEmpty() || v.getSubmissionId().isEmpty()) {
            return false;
        }
        if (!"ACCEPTED".equals(v.getResult())) {
            return false;
        }
        String phase = v.getPhase();
        if (phase == null || phase.isEmpty()) {
            return false;
        }
        if (!SCOREABLE_PHASES.contains(phase.trim().toLowerCase(Locale.ROOT))) {
            return false;
        }
        // Zero-point verdicts can't move the ZSET — skip the Lua round-trip.
        return v.getPoints() > 0;
    }

    /** {@code totalPoints * 10_000_000 - penaltyMinutes}. Mirrors {@code ScoreEncoder.encode}. */
    static double encode(long totalPoints, long penaltyMinutes) {
        return (double) (totalPoints * SCORE_MULTIPLIER - penaltyMinutes);
    }
}
