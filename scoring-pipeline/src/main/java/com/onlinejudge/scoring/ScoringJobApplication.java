package com.onlinejudge.scoring;

import com.esotericsoftware.kryo.Serializer;
import com.onlinejudge.common.sharding.ScoreRangeShardRouter;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
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
 *   Kafka (configured verdict topic)
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
 *   curl -X POST http://localhost:18081/jars/upload -F "jarfile=@build/libs/scoring-pipeline-all.jar"
 *   Then trigger the job via the Flink UI at http://localhost:18081
 */
public class ScoringJobApplication {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = getFirstEnv(
                new String[]{"SCORING_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP"},
                "localhost:9093");
        String verdictTopic   = getFirstEnv(
                new String[]{"SCORING_INPUT_TOPIC", "SCORING_KAFKA_TOPIC"},
                "regional.evaluated_results");
        String consumerGroup  = getEnv("SCORING_GROUP_ID", "scoring-pipeline");
        String redisHost      = getEnv("REDIS_HOST", "localhost");
        int    redisPort      = Integer.parseInt(getEnv("REDIS_PORT", "6379"));
        String checkpointDir  = getEnv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ScoreUpdate is a Java record. Flink's POJO type analyzer does not
        // recognise records (final fields, no setters) and falls back to Kryo.
        // Kryo's default FieldSerializer uses sun.misc.Unsafe field offsets,
        // which the JDK forbids on record fields — every sink emit would die
        // with "can't get field offset on a record class". Use Flink's own
        // classloader-aware JavaSerializer (the upstream Kryo one calls plain
        // ObjectInputStream and can't see user-code classes through Flink's
        // child-first classloader, manifesting as ClassNotFoundException on
        // deserialize). The record already implements Serializable.
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends Serializer<?>> kryoJavaSerializer = (Class) JavaSerializer.class;
        env.getConfig().addDefaultKryoSerializer(ScoreUpdate.class, kryoJavaSerializer);

        // Exactly-once checkpointing every 30 seconds
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        // Kafka source: verdict topic. Locally this defaults to the MM2-mirrored
        // global topic (`regional.evaluated_results`) rather than the regional
        // producer topic (`evaluated_results`).
        // Use SimpleDeserializationSchema to extract the raw byte[] value from each Kafka record
        KafkaSource<byte[]> kafkaSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(verdictTopic)
                .setGroupId(consumerGroup)
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
        // is `event_ts_ms` from the VerdictEvent proto, falling back to
        // `gateway_ts_ms` for older events.
        WatermarkStrategy<byte[]> watermarkStrategy = WatermarkStrategy
                .<byte[]>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((eventBytes, recordTimestamp) -> {
                    try {
                        com.onlinejudge.common.events.Events.VerdictEvent event =
                                com.onlinejudge.common.events.Events.VerdictEvent.parseFrom(eventBytes);
                        return event.getEventTsMs() > 0 ? event.getEventTsMs() : event.getGatewayTsMs();
                    } catch (Exception e) {
                        return recordTimestamp;
                    }
                });

        DataStream<byte[]> verdictStream = env
                .fromSource(kafkaSource, watermarkStrategy, verdictTopic);

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

    private static String getFirstEnv(String[] keys, String defaultValue) {
        for (String key : keys) {
            String value = System.getenv(key);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return defaultValue;
    }
}
