package com.traffic.flink;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DeviceStatusCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "device_status_group");

        DataStream<DeviceStatus> stream = env.addSource(
            new FlinkKafkaConsumer<>("device_status", new SimpleStringSchema(), props)
        ).map(line -> {
            String[] fields = line.split("\t");
            return new DeviceStatus(
                fields[0],
                fields[5],
                Double.parseDouble(fields[1]),
                Double.parseDouble(fields[2]),
                Double.parseDouble(fields[3])
            );
        }).assignTimestampsAndWatermarks(new DeviceWatermarkAssigner());

        Pattern<DeviceStatus, ?> offlinePattern = Pattern.<DeviceStatus>begin("first")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return "OFFLINE".equals(value.onlineFlag);
                }
            })
            .next("second")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return "OFFLINE".equals(value.onlineFlag);
                }
            })
            .within(Time.seconds(60));

        Pattern<DeviceStatus, ?> highCPU = Pattern.<DeviceStatus>begin("high_cpu")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.cpuUsage > 90;
                }
            })
            .times(3)
            .within(Time.minutes(5));

        DataStream<String> offlineAlert = CEP.pattern(stream.keyBy(d -> d.deviceId), offlinePattern)
            .select(new OfflineAlertSelector());

        DataStream<String> cpuAlert = CEP.pattern(stream.keyBy(d -> d.deviceId), highCPU)
            .select(new CPUAlertSelector());

        offlineAlert.union(cpuAlert).print();

        env.execute("Device Status CEP Monitoring");
    }

    public static class DeviceStatus {
        public String deviceId;
        public String onlineFlag;
        public double cpuUsage;
        public double memoryUsage;
        public double temperature;

        public DeviceStatus(String deviceId, String onlineFlag, double cpuUsage, double memoryUsage, double temperature) {
            this.deviceId = deviceId;
            this.onlineFlag = onlineFlag;
            this.cpuUsage = cpuUsage;
            this.memoryUsage = memoryUsage;
            this.temperature = temperature;
        }
    }

    private static class DeviceWatermarkAssigner implements AssignerWithPeriodicWatermarks<DeviceStatus> {
        private long currentMaxTimestamp = 0;

        @Override
        public long extractTimestamp(DeviceStatus element, long previousElementTimestamp) {
            long timestamp = System.currentTimeMillis();
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - 3000);
        }
    }

    private static class OfflineAlertSelector implements PatternSelectFunction<DeviceStatus, String> {
        @Override
        public String select(Map<String, List<DeviceStatus>> pattern) {
            DeviceStatus first = pattern.get("first").get(0);
            return String.format("ALERT: Device %s continuous offline detected", first.deviceId);
        }
    }

    private static class CPUAlertSelector implements PatternSelectFunction<DeviceStatus, String> {
        @Override
        public String select(Map<String, List<DeviceStatus>> pattern) {
            DeviceStatus status = pattern.get("high_cpu").get(0);
            return String.format("ALERT: Device %s CPU usage exceeds 90%%", status.deviceId);
        }
    }
}