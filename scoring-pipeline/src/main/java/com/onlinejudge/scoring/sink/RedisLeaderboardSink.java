package com.onlinejudge.scoring.sink;

import com.onlinejudge.scoring.model.ScoreUpdate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.List;

/**
 * Flink sink: writes score updates to Redis ZSET using an atomic Lua script.
 *
 * Key pattern: leaderboard:{contestId}
 * Score: composite float = (totalPoints * 10_000_000) - penaltyMinutes
 *
 * Atomicity: the Lua script executes as a single atomic Redis operation.
 * No concurrent ZADD for the same user can interleave between the read and write.
 *
 * After each ZADD, publishes a score-change event to Redis Pub/Sub channel
 * "score_updates:{contestId}" for WebSocket differential push.
 *
 * Production: score-range sharding would distribute buckets across 3+ nodes.
 * Local: single Redis node, single ZSET per contest. Same API, no sharding logic.
 */
public class RedisLeaderboardSink extends RichSinkFunction<ScoreUpdate> {

    private static final Logger log = LoggerFactory.getLogger(RedisLeaderboardSink.class);

    /**
     * Atomic Lua script: unconditionally sets the user's score to the new value.
     * Idempotent: replaying the same ScoreUpdate produces the same ZSET state.
     * Also publishes a score-change notification to Pub/Sub.
     */
    private static final String LUA_UPDATE_SCORE = """
            local leaderboard_key = KEYS[1]
            local pubsub_channel  = KEYS[2]
            local user_id         = ARGV[1]
            local new_score       = tonumber(ARGV[2])
            local contest_id      = ARGV[3]
            local total_score     = ARGV[4]
            local penalty         = ARGV[5]

            local old_rank = redis.call('ZREVRANK', leaderboard_key, user_id)
            redis.call('ZADD', leaderboard_key, new_score, user_id)
            local new_rank = redis.call('ZREVRANK', leaderboard_key, user_id)

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

    private transient JedisPool jedisPool;

    public RedisLeaderboardSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        log.info("[redis-sink] Connected to Redis {}:{}", redisHost, redisPort);
    }

    @Override
    public void invoke(ScoreUpdate update, Context context) {
        String leaderboardKey = "leaderboard:" + update.contestId();
        String pubsubChannel  = "score_updates:" + update.contestId();

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.eval(
                LUA_UPDATE_SCORE,
                List.of(leaderboardKey, pubsubChannel),
                Arrays.asList(
                    update.userId(),
                    String.valueOf(update.zsetScore()),
                    update.contestId(),
                    String.valueOf(update.totalScore()),
                    String.valueOf(update.penaltyMinutes())
                )
            );

            log.debug("[redis-sink] Updated leaderboard user={} contest={} score={} zset={}",
                    update.userId(), update.contestId(), update.totalScore(), update.zsetScore());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) jedisPool.close();
    }
}
