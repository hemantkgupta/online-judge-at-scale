package com.onlinejudge.scoring.sink;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import com.onlinejudge.scoring.model.ScoreUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Flink sink: writes score updates to Redis ZSET using an atomic Lua script,
 * routed to the correct score-range shard.
 *
 * <p>Key pattern: {@code leaderboard:{contestId}:shard:{shardIndex}} (via
 * {@link ScoreRangeShardRouter}).
 *
 * <p>Score: composite float = {@code (totalPoints * 10_000_000) - penaltyMinutes}.
 *
 * <p>When a user's score crosses a shard boundary (e.g. solves one more problem
 * and moves from the 0–29.999M shard to the 30M–59.999M shard), the Lua script
 * also {@code ZREM}s the user from every other shard before {@code ZADD}ing to
 * the new one. This keeps the user present in exactly one shard at any time,
 * which is the invariant the read-side global-rank computation relies on.
 *
 * <p>Atomicity: the Lua script executes as a single atomic Redis operation. No
 * concurrent ZADD for the same user can interleave between the cleanup and write.
 *
 * <p>After each ZADD, publishes a score-change event to Redis Pub/Sub channel
 * {@code score_updates:{contestId}} for WebSocket differential push.
 *
 * <p>Production: the shards map to distinct Redis nodes via Redis Cluster. Local:
 * single Redis node, multiple ZSET keys.
 */
public class RedisLeaderboardSink extends RichSinkFunction<ScoreUpdate> {

    private static final Logger log = LoggerFactory.getLogger(RedisLeaderboardSink.class);

    /**
     * Atomic Lua script:
     *   KEYS[1]    = Pub/Sub channel
     *   KEYS[2]    = target shard key (where the user's new score belongs)
     *   KEYS[3..N] = OTHER shard keys (to remove the user from in case of boundary crossing)
     *
     *   ARGV[1] = user_id
     *   ARGV[2] = new composite zsetScore
     *   ARGV[3] = contest_id
     *   ARGV[4] = totalPoints
     *   ARGV[5] = penaltyMinutes
     *
     * Idempotent: replaying the same ScoreUpdate produces the same ZSET state.
     */
    private static final String LUA_UPDATE_SCORE = """
            local pubsub_channel   = KEYS[1]
            local target_shard_key = KEYS[2]
            local user_id          = ARGV[1]
            local new_score        = tonumber(ARGV[2])
            local contest_id       = ARGV[3]
            local total_score      = ARGV[4]
            local penalty          = ARGV[5]

            for i = 3, #KEYS do
                redis.call('ZREM', KEYS[i], user_id)
            end

            redis.call('ZADD', target_shard_key, new_score, user_id)
            local new_rank = redis.call('ZREVRANK', target_shard_key, user_id)

            local msg = '{"userId":"' .. user_id
                      .. '","contestId":"' .. contest_id
                      .. '","newScore":' .. total_score
                      .. ',"penaltyMinutes":' .. penalty
                      .. ',"newRank":' .. (new_rank or 0)
                      .. '}'
            redis.call('PUBLISH', pubsub_channel, msg)
            return new_rank
            """;

    private final String redisHost;
    private final int redisPort;
    private final ScoreRangeShardRouter router;

    private transient JedisPool jedisPool;

    public RedisLeaderboardSink(String redisHost, int redisPort, ScoreRangeShardRouter router) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.router = router;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        log.info("[redis-sink] Connected to Redis {}:{} with {} score-range shards",
                redisHost, redisPort, router.shardCount());
    }

    @Override
    public void invoke(ScoreUpdate update, Context context) {
        String pubsubChannel = "score_updates:" + update.contestId();

        int targetShard = router.shardForScore(update.zsetScore());
        String targetShardKey = router.shardKey(update.contestId(), targetShard);

        // KEYS = [pubsub, targetShard, otherShard1, otherShard2, ...]
        List<String> keys = new ArrayList<>(router.shardCount() + 1);
        keys.add(pubsubChannel);
        keys.add(targetShardKey);
        for (int i = 0; i < router.shardCount(); i++) {
            if (i != targetShard) {
                keys.add(router.shardKey(update.contestId(), i));
            }
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.eval(
                LUA_UPDATE_SCORE,
                keys,
                Arrays.asList(
                    update.userId(),
                    String.valueOf(update.zsetScore()),
                    update.contestId(),
                    String.valueOf(update.totalScore()),
                    String.valueOf(update.penaltyMinutes())
                )
            );

            log.debug("[redis-sink] Updated leaderboard user={} contest={} shard={} score={}",
                    update.userId(), update.contestId(), targetShard, update.totalScore());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) jedisPool.close();
    }
}
