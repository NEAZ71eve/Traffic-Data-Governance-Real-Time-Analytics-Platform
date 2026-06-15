package com.traffic.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * 交通拥堵检测任务
 * 
 * 数据源：Kafka traffic_status
 * 计算逻辑：5分钟滚动窗口 + 键控状态(KeyedState)检测流量突增
 * 
 * 拥堵判定阈值（来自metrics_thresholds.json）：
 * - 拥堵等级3（轻度拥堵）：avg_speed 20-30 km/h，flow 1000-1500辆/小时
 * - 拥堵等级4（中度拥堵）：avg_speed 10-20 km/h，flow 1500-2000辆/小时
 * - 拥堵等级5（严重拥堵）：avg_speed <10 km/h，flow >2000辆/小时
 * 
 * 流量突增检测（KeyedState + Timer实现）：
 * - 当前5分钟窗口车流量 > 近7天同时段均值 * 2
 * - 触发"流量异常"告警 → 可能发生交通事故或大型活动
 * 
 * Watermark策略：允许30秒乱序（BoundedOutOfOrderness）
 */
public class TrafficCongestionDetection {

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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints/traffic-congestion");
        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(60)));

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "traffic-kafka-1:9092");
        props.setProperty("group.id", "traffic_status_group");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest");

        DataStream<String> stream = env.addSource(
            new FlinkKafkaConsumer<>("traffic_status", new SimpleStringSchema(), props)
        ).name("Kafka Source: traffic_status").uid("kafka-source-traffic-status");

        // ==========================================
        // 数据解析 + Watermark
        // fields: road_id | avg_speed | traffic_flow | jam_level | congestion_rate | peak_flag | sample_time
        // ==========================================
        DataStream<Tuple5<String, Integer, Integer, Double, String>> parsed = stream
            .map(line -> {
                String[] fields = line.split("\t");
                return Tuple5.of(
                    fields[0],                           // road_id
                    Integer.parseInt(fields[1]),         // avg_speed
                    Integer.parseInt(fields[2]),         // traffic_flow
                    Double.parseDouble(fields[4]),       // congestion_rate
                    fields[6]                            // sample_time
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple5<String, Integer, Integer, Double, String>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis())
                    .withIdleness(Duration.ofSeconds(5))
            );

        // ==========================================
        // 5分钟滚动窗口拥堵聚合
        // ==========================================
        DataStream<Tuple5<String, Integer, Integer, Double, String>> aggregated = parsed
            .keyBy(t -> t.f0)
            .timeWindow(Time.minutes(5))
            .reduce(new ReduceFunction<Tuple5<String, Integer, Integer, Double, String>>() {
                @Override
                public Tuple5<String, Integer, Integer, Double, String> reduce(
                    Tuple5<String, Integer, Integer, Double, String> v1,
                    Tuple5<String, Integer, Integer, Double, String> v2
                ) {
                    return Tuple5.of(
                        v1.f0,
                        Math.min(v1.f1, v2.f1),          // 最低速度
                        v1.f2 + v2.f2,                    // 累计流量
                        Math.max(v1.f3, v2.f3),            // 最高拥堵率
                        v1.f4                              // sample_time
                    );
                }
            })
            .name("Congestion Window Reduce").uid("congestion-window-reduce");

        // ==========================================
        // 流量突增检测（KeyedProcessFunction + State + Timer）
        // 
        // 原理：
        // 1. 每条记录到达时，更新KeyedState中的历史流量均值
        // 2. 当前窗口流量 > 历史均值 * 2 时，触发告警
        // 3. Timer用于定期清理过期状态（24小时后）
        // ==========================================
        DataStream<String> anomalyAlerts = aggregated
            .keyBy(t -> t.f0)
            .process(new FlowAnomalyDetector())
            .name("Flow Anomaly Detector").uid("flow-anomaly-detector");

        anomalyAlerts.print().name("Anomaly Alert Printer").uid("anomaly-alert-printer");

        env.execute("Traffic Congestion Detection");
    }

    /**
     * 流量突增异常检测器
     * 
     * 使用KeyedState维护历史流量均值（近7天）
     * 规则：当前窗口流量 > 历史均值 * 2 → 流量异常告警
     */
    private static class FlowAnomalyDetector extends KeyedProcessFunction<String, Tuple5<String, Integer, Integer, Double, String>, String> {

        private ValueState<Double> historicalAvgFlow; // 历史均值状态
        private ValueState<Integer> sampleCount;       // 样本计数

        @Override
        public void open(Configuration parameters) {
            historicalAvgFlow = getRuntimeContext().getState(
                new ValueStateDescriptor<>("historical-avg-flow", Double.class));
            sampleCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sample-count", Integer.class));
        }

        @Override
        public void processElement(
            Tuple5<String, Integer, Integer, Double, String> value,
            Context ctx,
            Collector<String> out
        ) throws Exception {
            Double historyAvg = historicalAvgFlow.value();
            Integer count = sampleCount.value();

            if (historyAvg == null) {
                // 首次初始化
                historicalAvgFlow.update((double) value.f2);
                sampleCount.update(1);
            } else {
                int currentFlow = value.f2;

                // 流量突增检测：当前 > 历史均值 * 2
                if (currentFlow > historyAvg * 2 && count >= 5) {
                    out.collect(String.format(
                        "ANOMALY|FLOW_SPIKE|Road %s flow=%d, history_avg=%.0f, ratio=%.2f",
                        value.f0, currentFlow, historyAvg, currentFlow / historyAvg
                    ));
                }

                // 指数移动平均更新历史值（EMA, alpha=0.2）
                double newAvg = historyAvg * 0.8 + currentFlow * 0.2;
                historicalAvgFlow.update(newAvg);
                sampleCount.update(count + 1);
            }

            // 注册24小时后的清理Timer
            ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 24 * 60 * 60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            // 清理过期状态（24小时后不再需要）
            historicalAvgFlow.clear();
            sampleCount.clear();
        }
    }
}
