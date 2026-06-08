package com.traffic.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class TrafficVehicleCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "traffic_vehicle_group");

        DataStream<String> stream = env.addSource(
            new FlinkKafkaConsumer<>("traffic_vehicle", new SimpleStringSchema(), props)
        );

        DataStream<Tuple4<String, Long, Integer, Double>> result = stream
            .map(line -> {
                String[] fields = line.split("\t");
                return Tuple3.of(
                    fields[1], 
                    Long.parseLong(fields[4]), 
                    Integer.parseInt(fields[5])
                );
            })
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {
                private long currentMaxTimestamp = 0;
                private final long maxOutOfOrderness = 5000;

                @Override
                public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                    long timestamp = element.f1;
                    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                    return timestamp;
                }

                @Override
                public Watermark getCurrentWatermark() {
                    return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                }
            })
            .keyBy(0)
            .timeWindow(Time.minutes(5))
            .aggregate(new VehicleCountAggregate());

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("redis")
            .setPort(6379)
            .build();

        result.addSink(new RedisSink<>(redisConfig, new VehicleCountRedisMapper()));

        env.execute("Traffic Vehicle Real-time Count");
    }

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
                accumulator.f0 + 1,
                accumulator.f1 + value.f2,
                accumulator.f2 + 1
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