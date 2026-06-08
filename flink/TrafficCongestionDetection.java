package com.traffic.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class TrafficCongestionDetection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "traffic_status_group");

        DataStream<String> stream = env.addSource(
            new FlinkKafkaConsumer<>("traffic_status", new SimpleStringSchema(), props)
        );

        DataStream<Tuple5<String, Integer, Double, Integer, String>> result = stream
            .map(line -> {
                String[] fields = line.split("\t");
                return Tuple5.of(
                    fields[0],
                    Integer.parseInt(fields[1]),
                    Double.parseDouble(fields[3]),
                    Integer.parseInt(fields[2]),
                    fields[7]
                );
            })
            .assignTimestampsAndWatermarks(new StatusWatermarkAssigner())
            .keyBy(0)
            .timeWindow(Time.minutes(5))
            .reduce(new CongestionReduceFunction());

        result.print();

        env.execute("Traffic Congestion Detection");
    }

    private static class StatusWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple5<String, Integer, Double, Integer, String>> {
        private long currentMaxTimestamp = 0;
        private final long maxDelay = 3000;

        @Override
        public long extractTimestamp(Tuple5<String, Integer, Double, Integer, String> element, long previousElementTimestamp) {
            long timestamp = System.currentTimeMillis();
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxDelay);
        }
    }

    private static class CongestionReduceFunction implements ReduceFunction<Tuple5<String, Integer, Double, Integer, String>> {
        @Override
        public Tuple5<String, Integer, Double, Integer, String> reduce(
            Tuple5<String, Integer, Double, Integer, String> value1,
            Tuple5<String, Integer, Double, Integer, String> value2
        ) {
            return Tuple5.of(
                value1.f0,
                Math.min(value1.f1, value2.f1),
                Math.max(value1.f2, value2.f2),
                value1.f3 + value2.f3,
                value1.f4
            );
        }
    }
}