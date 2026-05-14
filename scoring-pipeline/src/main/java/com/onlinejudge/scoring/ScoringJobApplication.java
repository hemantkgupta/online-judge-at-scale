package com.onlinejudge.scoring;

import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import com.onlinejudge.scoring.function.ScoringFunction;
import com.onlinejudge.scoring.model.ScoreUpdate;
import com.onlinejudge.scoring.sink.RedisLeaderboardSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Apache Flink job: stateful contest scoring pipeline.
 *
 * Pipeline:
 *   Kafka (evaluated_results)
 *     --> keyBy(userId)
 *     --> ScoringFunction (KeyedProcessFunction)
 *         - ValueState<ScoringState> per user
 *         - Event-time with BoundedOutOfOrderness(5 min)
 *     --> RedisLeaderboardSink (atomic Lua ZADD + Pub/Sub notify)
 *
 * Exactly-once: Flink ABS checkpointing to local filesystem (dev).
 * Production: checkpoint to S3 / HDFS. Kafka transactional producers.
 *
 * Submit this fat JAR to the local Flink cluster via:
 *   curl -X POST http://localhost:8081/jars/upload -F "jarfile=@build/libs/scoring-pipeline-all.jar"
 *   Then trigger the job via the Flink UI at http://localhost:8081
 */
public class ScoringJobApplication {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = getEnv("KAFKA_BOOTSTRAP", "localhost:9092");
        String redisHost      = getEnv("REDIS_HOST", "localhost");
        int    redisPort      = Integer.parseInt(getEnv("REDIS_PORT", "6379"));
        String checkpointDir  = getEnv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Exactly-once checkpointing every 30 seconds
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        // Kafka source: evaluated_results topic
        // Use SimpleDeserializationSchema to extract the raw byte[] value from each Kafka record
        KafkaSource<byte[]> kafkaSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("evaluated_results")
                .setGroupId("scoring-pipeline")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<byte[]>() {
                    @Override
                    public byte[] deserialize(byte[] message) {
                        return message;
                    }
                    @Override
                    public boolean isEndOfStream(byte[] nextElement) {
                        return false;
                    }
                    @Override
                    public TypeInformation<byte[]> getProducedType() {
                        return TypeInformation.of(byte[].class);
                    }
                })
                .build();

        // Event-time watermark: BoundedOutOfOrderness 5 minutes
        // Keeps scoring window open after contest close to absorb late-arriving verdicts
        // from VMs that were executing at contest close time. The event timestamp
        // is `gateway_ts_ms` from the VerdictEvent proto — the API Gateway T0 stamp
        // that travels through the whole pipeline (Part 2 of the blog).
        WatermarkStrategy<byte[]> watermarkStrategy = WatermarkStrategy
                .<byte[]>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((eventBytes, recordTimestamp) -> {
                    try {
                        return com.onlinejudge.common.events.Events.VerdictEvent
                                .parseFrom(eventBytes).getGatewayTsMs();
                    } catch (Exception e) {
                        return recordTimestamp;
                    }
                });

        DataStream<byte[]> verdictStream = env
                .fromSource(kafkaSource, watermarkStrategy, "evaluated_results");

        // keyBy userId — proto-decoded from VerdictEvent — and process with stateful ScoringFunction.
        DataStream<ScoreUpdate> scoreUpdates = verdictStream
                .keyBy(eventBytes -> {
                    try {
                        return com.onlinejudge.common.events.Events.VerdictEvent
                                .parseFrom(eventBytes).getUserId();
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .process(new ScoringFunction());

        // Sink: Redis ZSET via atomic Lua script + Pub/Sub notification,
        // routed to score-range shards via the shared ScoreRangeShardRouter.
        ScoreRangeShardRouter shardRouter = ScoreRangeShardRouter.defaultIcpcRouter();
        scoreUpdates.addSink(new RedisLeaderboardSink(redisHost, redisPort, shardRouter))
                .name("redis-leaderboard-sink");

        env.execute("Online Judge Scoring Pipeline");
    }

    private static String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
