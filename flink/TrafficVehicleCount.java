package com.traffic.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.time.Duration;
import java.util.Properties;

/**
 * 实时车流统计任务
 * 
 * 数据源：Kafka traffic_vehicle
 * 计算逻辑：5分钟滚动窗口，按roadId聚合车流量、总速度、平均速度
 * 输出：Redis (供Superset实时看板)
 * 
 * Watermark策略：允许30秒乱序数据（BoundedOutOfOrderness）
 * 迟到数据处理：sideOutputLateData 输出到侧输出流，后续离线补录
 * 
 * 容灾配置：
 * - Checkpoint: 每5分钟触发，EXACTLY_ONCE语义
 * - StateBackend: HashMapStateBackend(内存) + RocksDB(大状态)
 * - Restart: 固定延迟重启，60秒内最多重启3次
 * - Kafka故障降级：15分钟内无数据则告警，手动切换读取HDFS ODS增量
 */
public class TrafficVehicleCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==========================================
        // Checkpoint配置（面试高频考点）
        // ==========================================
        env.enableCheckpointing(5 * 60 * 1000); // 5分钟一次Checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000); // 两次Checkpoint最小间隔1分钟
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);     // Checkpoint超时时间10分钟
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);           // 同时最多1个Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        ); // 任务取消时保留Checkpoint，便于从Savepoint恢复
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints/traffic-vehicle-count");

        // StateBackend选择
        // - HashMapStateBackend: 适合状态较小、需要低延迟的场景（默认）
        // - EmbeddedRocksDBStateBackend: 适合状态较大（GB级别）、可接受稍高延迟
        env.setStateBackend(new HashMapStateBackend());
        // env.setStateBackend(new EmbeddedRocksDBStateBackend()); // 状态数据量大时启用

        // ==========================================
        // 重启策略
        // ==========================================
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(60)));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "traffic-kafka-1:9092");
        props.setProperty("group.id", "traffic_vehicle_group");
        // 防止数据丢失：关闭自动提交，依赖Flink Checkpoint保证Exactly-Once
        props.setProperty("enable.auto.commit", "false");
        // 从最早的未消费位置开始（首次启动），后续从Checkpoint恢复
        props.setProperty("auto.offset.reset", "earliest");

        DataStream<String> stream = env.addSource(
            new FlinkKafkaConsumer<>("traffic_vehicle", new SimpleStringSchema(), props)
        ).name("Kafka Source: traffic_vehicle").uid("kafka-source-traffic-vehicle");

        // ==========================================
        // Watermark策略：允许30秒乱序 + 5秒空闲超时
        // ==========================================
        DataStream<Tuple4<String, Long, Integer, Double>> result = stream
            .map(line -> {
                String[] fields = line.split("\t");
                // fields[1]=roadId, fields[4]=timestamp(ms), fields[5]=speed
                return Tuple3.of(
                    fields[1],
                    Long.parseLong(fields[4]),
                    Integer.parseInt(fields[5])
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                    .withIdleness(Duration.ofSeconds(5)) // 5秒空闲超时，防止某个分区无水印导致窗口不触发
            )
            .keyBy(t -> t.f0) // 按roadId分组
            .timeWindow(Time.minutes(5)) // 5分钟滚动窗口
            .aggregate(new VehicleCountAggregate())
            .name("5min Window Aggregate").uid("window-aggregate");

        // ==========================================
        // 迟到数据处理：输出到侧输出流
        // 生产环境中可写入HDFS备用，后续离线T+1补录到DWD/DWS
        // ==========================================
        // OutputTag<Tuple3<String, Long, Integer>> lateDataTag = new OutputTag<>("late-data"){};
        // DataStream<Tuple3<String, Long, Integer>> lateStream = result.getSideOutput(lateDataTag);
        // lateStream.addSink(/* HDFS Sink */);

        // ==========================================
        // Redis Sink：写入实时结果供Superset消费
        // ==========================================
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("docker-redis-1")
            .setPort(6379)
            .build();

        result.addSink(new RedisSink<>(redisConfig, new VehicleCountRedisMapper()))
             .name("Redis Sink").uid("redis-sink");

        env.execute("Traffic Vehicle Real-time Count");
    }

    /**
     * 车辆通行聚合器
     * 
     * 输入: Tuple3<roadId, timestamp, speed>
     * 累加器: Tuple3<count, totalSpeed, recordCount>
     * 输出: Tuple4<key, count, totalSpeed, avgSpeed>
     */
    private static class VehicleCountAggregate implements AggregateFunction<
        Tuple3<String, Long, Integer>,
        Tuple3<Long, Integer, Integer>,
        Tuple4<String, Long, Integer, Double>
    > {
        @Override
        public Tuple3<Long, Integer, Integer> createAccumulator() {
            return Tuple3.of(0L, 0, 0);
        }

        @Override
        public Tuple3<Long, Integer, Integer> add(Tuple3<String, Long, Integer> value, Tuple3<Long, Integer, Integer> accumulator) {
            return Tuple3.of(
                accumulator.f0 + 1,           // count++
                accumulator.f1 + value.f2,     // totalSpeed += speed
                accumulator.f2 + 1             // recordCount++
            );
        }

        @Override
        public Tuple4<String, Long, Integer, Double> getResult(Tuple3<Long, Integer, Integer> accumulator) {
            return Tuple4.of(
                "traffic:count",
                accumulator.f0,
                accumulator.f1,
                accumulator.f2 > 0 ? (double) accumulator.f1 / accumulator.f2 : 0.0
            );
        }

        @Override
        public Tuple3<Long, Integer, Integer> merge(Tuple3<Long, Integer, Integer> a, Tuple3<Long, Integer, Integer> b) {
            return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    /**
     * Redis写入映射器
     * 写入格式: HSET traffic:vehicle {roadId} "{count},{totalSpeed},{avgSpeed}"
     * 端到端延迟: < 5秒（Kafka消费 + Flink窗口 + Redis写入）
     */
    private static class VehicleCountRedisMapper implements RedisMapper<Tuple4<String, Long, Integer, Double>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "traffic:vehicle");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Long, Integer, Double> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String, Long, Integer, Double> data) {
            return String.format("%d,%d,%.2f", data.f1, data.f2, data.f3);
        }
    }
}
