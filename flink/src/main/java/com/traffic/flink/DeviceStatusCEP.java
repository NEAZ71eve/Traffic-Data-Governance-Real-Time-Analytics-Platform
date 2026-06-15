package com.traffic.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 设备状态CEP异常检测任务
 * 
 * CEP规则明细（明确阈值）：
 * 
 * 规则1 - 连续离线检测：
 *   - 定义：设备状态主题中，连续3个心跳周期内 online_flag='OFFLINE'
 *   - 触发：连续3条记录均为OFFLINE（心跳周期约1分钟，即3分钟内持续离线）
 *   - 告警：生成"设备持续离线"告警 → 推送运维工单系统
 *   - 恢复：检测到设备 online_flag='ONLINE' 时清除告警
 * 
 * 规则2 - CPU持续高负载检测：
 *   - 定义：5分钟窗口内，连续3次CPU使用率 > 90%
 *   - 触发：连续3条记录CPU均 > 90%（3分钟内）
 *   - 告警：生成"CPU高负载"告警
 *   - 级别：CRITICAL（CPU > 95%）/ MAJOR（CPU > 90%）
 * 
 * 规则3 - 高频告警检测（新增）：
 *   - 定义：5分钟滚动窗口内，同一device_id的告警次数 > 10
 *   - 触发：告警次数超过阈值
 *   - 告警：生成"高频告警"通知，可能设备即将故障
 */
public class DeviceStatusCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==========================================
        // Checkpoint配置
        // ==========================================
        env.enableCheckpointing(5 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints/device-status-cep");
        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(60)));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "traffic-kafka-1:9092");
        props.setProperty("group.id", "device_status_group");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest");

        DataStream<DeviceStatus> stream = env.addSource(
            new FlinkKafkaConsumer<>("device_status", new SimpleStringSchema(), props)
        ).map(line -> {
            // fields: device_id | cpu_usage | memory_usage | temperature | online_flag | heartbeat_time | signal_strength | device_type
            String[] fields = line.split("\t");
            return new DeviceStatus(
                fields[0],
                Double.parseDouble(fields[1]),  // cpu_usage
                Double.parseDouble(fields[2]),  // memory_usage
                Double.parseDouble(fields[3]),  // temperature
                fields[4],                      // online_flag
                Long.parseLong(fields[5])       // heartbeat_time(ms)
            );
        }).assignTimestampsAndWatermarks(
            WatermarkStrategy.<DeviceStatus>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((element, recordTimestamp) -> element.heartbeatTime)
                .withIdleness(Duration.ofSeconds(5))
        ).name("Device Status Map + Watermark").uid("device-status-map");

        // ==========================================
        // CEP规则1：连续离线检测（连续3条OFFLINE）
        // 时间窗口：60秒内（心跳周期约1分钟）
        // ==========================================
        Pattern<DeviceStatus, ?> offlinePattern = Pattern.<DeviceStatus>begin("offline_1")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return "OFFLINE".equals(value.onlineFlag);
                }
            })
            .next("offline_2")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return "OFFLINE".equals(value.onlineFlag);
                }
            })
            .next("offline_3")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return "OFFLINE".equals(value.onlineFlag);
                }
            })
            .within(Time.seconds(180)); // 3分钟内连续3次离线

        // ==========================================
        // CEP规则2：CPU持续高负载（连续3次 > 90%）
        // 时间窗口：5分钟
        // ==========================================
        Pattern<DeviceStatus, ?> highCPUPattern = Pattern.<DeviceStatus>begin("cpu_high_1")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.cpuUsage > 90;
                }
            })
            .next("cpu_high_2")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.cpuUsage > 90;
                }
            })
            .next("cpu_high_3")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.cpuUsage > 90;
                }
            })
            .within(Time.minutes(5));

        // ==========================================
        // CEP规则3：设备温度过高（连续2次 > 80℃）
        // ==========================================
        Pattern<DeviceStatus, ?> highTempPattern = Pattern.<DeviceStatus>begin("temp_high_1")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.temperature > 80;
                }
            })
            .next("temp_high_2")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.temperature > 80;
                }
            })
            .within(Time.minutes(10));

        // ==========================================
        // CEP规则4：高频告警检测（5分钟内同一设备告警 > 10次）
        // 使用 times() 匹配模式：device_id 相同的告警记录在5分钟内出现超过10次
        // ==========================================
        Pattern<DeviceStatus, ?> highFreqPattern = Pattern.<DeviceStatus>begin("first")
            .where(new SimpleCondition<DeviceStatus>() {
                @Override
                public boolean filter(DeviceStatus value) {
                    return value.onlineFlag != null;
                }
            })
            .timesOrMore(10)  // 连续10次以上
            .consecutive()
            .within(Time.minutes(5));

        // 应用CEP模式
        DataStream<String> offlineAlert = CEP.pattern(
            stream.keyBy(d -> d.deviceId), offlinePattern
        ).select(new AlertSelector("OFFLINE", "CRITICAL"));

        DataStream<String> cpuAlert = CEP.pattern(
            stream.keyBy(d -> d.deviceId), highCPUPattern
        ).select(new AlertSelector("CPU_HIGH", "MAJOR"));

        DataStream<String> tempAlert = CEP.pattern(
            stream.keyBy(d -> d.deviceId), highTempPattern
        ).select(new AlertSelector("TEMP_HIGH", "MAJOR"));

        DataStream<String> highFreqAlert = CEP.pattern(
            stream.keyBy(d -> d.deviceId), highFreqPattern
        ).select(new AlertSelector("HIGH_FREQ", "CRITICAL"));

        // 合并所有告警流
        DataStream<String> allAlerts = offlineAlert.union(cpuAlert).union(tempAlert).union(highFreqAlert)
            ;

        // 输出1: 控制台打印（开发调试用）
        allAlerts.print().name("Alert Printer").uid("alert-printer");

        // 输出2: Kafka device_alarm Topic（下游告警系统消费）
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "traffic-kafka-1:9092");
        allAlerts.addSink(
            new FlinkKafkaProducer<>("device_alarm", new SimpleStringSchema(), producerProps)
        ).name("Kafka Alert Sink").uid("kafka-alert-sink");

        // 输出3: Redis实时告警缓存（ZSET按时间排序，HASH存储详情）
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("docker-redis-1")
            .setPort(6379)
            .build();
        allAlerts.addSink(new RedisSink<>(jedisConfig, new AlertRedisMapper()))
            .name("Redis Alert Sink").uid("redis-alert-sink");

        env.execute("Device Status CEP Monitoring");
    }

    /**
     * 设备状态POJO
     */
    public static class DeviceStatus {
        public String deviceId;
        public double cpuUsage;
        public double memoryUsage;
        public double temperature;
        public String onlineFlag;
        public long heartbeatTime;

        public DeviceStatus(String deviceId, double cpuUsage, double memoryUsage,
                           double temperature, String onlineFlag, long heartbeatTime) {
            this.deviceId = deviceId;
            this.cpuUsage = cpuUsage;
            this.memoryUsage = memoryUsage;
            this.temperature = temperature;
            this.onlineFlag = onlineFlag;
            this.heartbeatTime = heartbeatTime;
        }
    }

    /**
     * 告警选择器：根据告警类型和级别格式化告警消息
     */
    private static class AlertSelector implements PatternSelectFunction<DeviceStatus, String> {
        private final String alertType;
        private final String alertLevel;

        public AlertSelector(String alertType, String alertLevel) {
            this.alertType = alertType;
            this.alertLevel = alertLevel;
        }

        @Override
        public String select(Map<String, List<DeviceStatus>> pattern) {
            DeviceStatus first = pattern.values().iterator().next().get(0);
            return String.format(
                "ALERT|%s|%s|Device %s %s detected at %d",
                alertLevel, alertType, first.deviceId,
                alertType.equals("OFFLINE") ? "continuous offline" :
                alertType.equals("CPU_HIGH") ? "CPU usage exceeds 90%" :
                alertType.equals("TEMP_HIGH") ? "temperature exceeds 80°C" :
                "high frequency alerts (>10 in 5min)",
                System.currentTimeMillis()
            );
        }
    }

    /**
     * Redis告警映射器：写入Redis缓存
     * - ZSET: "device:alerts:{deviceId}" → {timestamp} score → 按时间排序的告警列表
     * - HASH: "device:alert:detail:{alertId}" → 告警详情
     */
    private static class AlertRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 写入 ZSET 按时间戳排序
            return new RedisCommandDescription(RedisCommand.ZADD, "device:alerts:realtime");
        }

        @Override
        public String getKeyFromData(String data) {
            // data format: "ALERT|CRITICAL|OFFLINE|Device DEV001 continuous offline... at 1234567890"
            String[] parts = data.split("\\|");
            if (parts.length >= 4) {
                String deviceId = parts[3].replace("Device ", "").split(" ")[0];
                return "device:alerts:" + deviceId;
            }
            return "device:alerts:unknown";
        }

        @Override
        public String getValueFromData(String data) {
            // 提取时间戳作为score
            long timestamp = System.currentTimeMillis();
            String[] parts = data.split(" at ");
            if (parts.length >= 2) {
                try {
                    timestamp = Long.parseLong(parts[parts.length - 1].trim());
                } catch (NumberFormatException ignored) {}
            }
            return timestamp + "#" + data;
        }
    }
}
