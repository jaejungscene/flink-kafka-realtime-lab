package com.example.realtimelab.job;

import com.example.realtimelab.model.AggregateEvent;
import com.example.realtimelab.model.AlertEvent;
import com.example.realtimelab.model.DlqEvent;
import com.example.realtimelab.model.MerchantRiskProfile;
import com.example.realtimelab.model.TransactionEvent;
import com.example.realtimelab.rule.RiskRules;
import com.example.realtimelab.serde.JsonSerializationSchema;
import com.example.realtimelab.serde.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RealTimeAlertJob {
    private static final OutputTag<DlqEvent> DLQ_TAG = new OutputTag<>("dlq") {
    };
    private static final OutputTag<TransactionEvent> LATE_EVENT_TAG = new OutputTag<>("late-events") {
    };

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrapServers = params.getOrDefault("bootstrapServers", "kafka:9092");
        String rawTopic = params.getOrDefault("rawTopic", "transactions.raw");
        String replayTopic = params.getOrDefault("replayTopic", "transactions.replay");
        String merchantRiskProfileTopic = params.getOrDefault("merchantRiskProfileTopic", "merchant_risk_profiles");
        String alertTopic = params.getOrDefault("alertTopic", "alerts.fraud");
        String aggregateTopic = params.getOrDefault("aggregateTopic", "transactions.aggregates");
        String dlqTopic = params.getOrDefault("dlqTopic", "transactions.dlq");
        String consumerGroup = params.getOrDefault("consumerGroup", "flink-realtime-lab");
        long checkpointIntervalMillis = longParam(params, "checkpointIntervalMillis", 10_000L);
        Duration watermarkDelay = Duration.ofSeconds(longParam(params, "watermarkDelaySeconds", 10L));
        Duration allowedLateness = Duration.ofSeconds(longParam(params, "allowedLatenessSeconds", 30L));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointIntervalMillis);

        KafkaSource<String> rawSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(List.of(rawTopic, replayTopic))
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> merchantProfileSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(merchantRiskProfileTopic)
                .setGroupId(consumerGroup + "-merchant-profiles")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<TransactionEvent> parsedEvents = env
                .fromSource(rawSource, WatermarkStrategy.noWatermarks(), "kafka-transactions-raw")
                .process(new TransactionParser(DLQ_TAG, rawTopic + "," + replayTopic, replayTopic))
                .name("parse-and-validate-transactions");

        SingleOutputStreamOperator<MerchantRiskProfile> merchantProfiles = env
                .fromSource(merchantProfileSource, WatermarkStrategy.noWatermarks(), "kafka-merchant-risk-profiles")
                .process(new MerchantRiskProfileParser(DLQ_TAG, merchantRiskProfileTopic))
                .name("parse-merchant-risk-profiles");

        MapStateDescriptor<String, MerchantRiskProfile> merchantProfileState = merchantProfileStateDescriptor();
        BroadcastStream<MerchantRiskProfile> merchantProfileBroadcast = merchantProfiles.broadcast(merchantProfileState);

        SingleOutputStreamOperator<TransactionEvent> enrichedEvents = parsedEvents
                .connect(merchantProfileBroadcast)
                .process(new MerchantRiskProfileEnrichmentFunction(merchantProfileState))
                .name("enrich-with-merchant-risk-profiles");

        SingleOutputStreamOperator<TransactionEvent> events = enrichedEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TransactionEvent>forBoundedOutOfOrderness(watermarkDelay)
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime()))
                .process(new LateEventRouter(allowedLateness, rawTopic, replayTopic))
                .name("event-time-watermarks");

        parsedEvents
                .getSideOutput(DLQ_TAG)
                .sinkTo(kafkaSink(bootstrapServers, dlqTopic, dlqKey()))
                .name("sink-dlq");

        merchantProfiles
                .getSideOutput(DLQ_TAG)
                .sinkTo(kafkaSink(bootstrapServers, dlqTopic, dlqKey()))
                .name("sink-reference-data-dlq");

        events
                .filter(new HighRiskFilter())
                .map(new HighRiskAlertMapper())
                .sinkTo(kafkaSink(bootstrapServers, alertTopic, AlertEvent::getKey))
                .name("sink-high-risk-alerts");

        SingleOutputStreamOperator<AlertEvent> userWindowAlerts = events
                .keyBy(TransactionEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE_EVENT_TAG)
                .process(new UserWindowAlertFunction())
                .name("user-window-alerts");

        userWindowAlerts
                .sinkTo(kafkaSink(bootstrapServers, alertTopic, AlertEvent::getKey))
                .name("sink-user-window-alerts");

        SingleOutputStreamOperator<AggregateEvent> aggregates = events
                .keyBy(RealTimeAlertJob::aggregateKey)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE_EVENT_TAG)
                .aggregate(new TransactionStatsAggregate(), new TransactionAggregateWindowFunction())
                .name("country-category-merchant-aggregates");

        aggregates
                .sinkTo(kafkaSink(bootstrapServers, aggregateTopic, AggregateEvent::getKey))
                .name("sink-transaction-aggregates");

        SingleOutputStreamOperator<AlertEvent> merchantAnomalyAlerts = events
                .keyBy(event -> normalize(event.getMerchantId(), "merchant-unknown"))
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE_EVENT_TAG)
                .aggregate(new TransactionStatsAggregate(), new MerchantAnomalyWindowFunction())
                .name("merchant-anomaly-alerts");

        merchantAnomalyAlerts
                .sinkTo(kafkaSink(bootstrapServers, alertTopic, AlertEvent::getKey))
                .name("sink-merchant-anomaly-alerts");

        DataStream<DlqEvent> windowLateEvents = userWindowAlerts
                .getSideOutput(LATE_EVENT_TAG)
                .union(aggregates.getSideOutput(LATE_EVENT_TAG), merchantAnomalyAlerts.getSideOutput(LATE_EVENT_TAG))
                .map(new LateEventDlqMapper(rawTopic, replayTopic));

        events
                .getSideOutput(DLQ_TAG)
                .union(windowLateEvents)
                .sinkTo(kafkaSink(bootstrapServers, dlqTopic, dlqKey()))
                .name("sink-late-events-dlq");

        env.execute("flink-kraft-realtime-lab");
    }

    private static String aggregateKey(TransactionEvent event) {
        String country = normalize(event.getCountry(), "UNKNOWN");
        String category = normalize(event.getCategory(), "uncategorized");
        String merchant = normalize(event.getMerchantId(), "merchant-unknown");
        return country + "|" + category + "|" + merchant;
    }

    private static String normalize(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static MapStateDescriptor<String, MerchantRiskProfile> merchantProfileStateDescriptor() {
        return new MapStateDescriptor<>(
                "merchant-risk-profiles",
                String.class,
                MerchantRiskProfile.class);
    }

    private static <T> KafkaSink<T> kafkaSink(String bootstrapServers, String topic, KeyExtractor<T> keyExtractor) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new StringKeySerializationSchema<>(keyExtractor))
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build())
                .build();
    }

    private static KeyExtractor<DlqEvent> dlqKey() {
        return event -> normalize(event.getErrorType(), "DLQ");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (!arg.startsWith("--")) {
                continue;
            }
            String key = arg.substring(2);
            if (index + 1 < args.length && !args[index + 1].startsWith("--")) {
                params.put(key, args[++index]);
            } else {
                params.put(key, "true");
            }
        }
        return params;
    }

    private static long longParam(Map<String, String> params, String key, long fallback) {
        String value = params.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Long.parseLong(value);
    }

    @FunctionalInterface
    interface KeyExtractor<T> extends Serializable {
        String key(T element);
    }

    static class StringKeySerializationSchema<T> implements SerializationSchema<T> {
        private final KeyExtractor<T> keyExtractor;

        StringKeySerializationSchema(KeyExtractor<T> keyExtractor) {
            this.keyExtractor = keyExtractor;
        }

        @Override
        public byte[] serialize(T element) {
            String key = keyExtractor.key(element);
            if (key == null || key.isBlank()) {
                return null;
            }
            return key.getBytes(StandardCharsets.UTF_8);
        }
    }

    static class LateEventRouter extends ProcessFunction<TransactionEvent, TransactionEvent> {
        private final long allowedLatenessMillis;
        private final String rawTopic;
        private final String replayTopic;
        private transient ObjectMapper mapper;

        LateEventRouter(Duration allowedLateness, String rawTopic, String replayTopic) {
            this.allowedLatenessMillis = allowedLateness.toMillis();
            this.rawTopic = rawTopic;
            this.replayTopic = replayTopic;
        }

        @Override
        public void processElement(TransactionEvent event, Context context, Collector<TransactionEvent> out) throws Exception {
            long currentWatermark = context.timerService().currentWatermark();
            if (currentWatermark != Long.MIN_VALUE
                    && event.getEventTime() + allowedLatenessMillis < currentWatermark) {
                if (mapper == null) {
                    mapper = ObjectMapperFactory.create();
                }
                context.output(DLQ_TAG, lateEventDlq(event, rawTopic, replayTopic, mapper));
                return;
            }
            out.collect(event);
        }
    }

    static class HighRiskFilter implements FilterFunction<TransactionEvent> {
        @Override
        public boolean filter(TransactionEvent event) {
            return RiskRules.isHighRisk(event);
        }
    }

    static class HighRiskAlertMapper implements MapFunction<TransactionEvent, AlertEvent> {
        @Override
        public AlertEvent map(TransactionEvent event) {
            String riskTier = normalize(event.getMerchantRiskTier(), "UNKNOWN");
            double effectiveFraudScore = RiskRules.effectiveFraudScore(event);
            return AlertEvent.of(
                    "HIGH_RISK_TRANSACTION",
                    "CRITICAL",
                    event.getUserId(),
                    "single event exceeded fraud rule threshold; merchantRiskTier="
                            + riskTier
                            + ", merchantRiskMultiplier="
                            + String.format("%.3f", event.getMerchantRiskMultiplier())
                            + ", effectiveFraudScore="
                            + String.format("%.4f", effectiveFraudScore),
                    event.getEventTime(),
                    event.getEventTime(),
                    event.getEventTime(),
                    effectiveFraudScore,
                    event.getEventId());
        }
    }

    static class UserWindowAlertFunction extends ProcessWindowFunction<TransactionEvent, AlertEvent, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<TransactionEvent> events, Collector<AlertEvent> out) {
            long count = 0;
            double totalAmount = 0.0;
            double maxScore = 0.0;
            String sampleEventId = null;

            for (TransactionEvent event : events) {
                count++;
                totalAmount += event.getAmount();
                maxScore = Math.max(maxScore, event.getMlFraudScore());
                if (sampleEventId == null) {
                    sampleEventId = event.getEventId();
                }
            }

            if (RiskRules.isBurst(count, totalAmount)) {
                String reason = "user window exceeded count or amount threshold; count="
                        + count
                        + ", totalAmount="
                        + String.format("%.2f", totalAmount);

                out.collect(AlertEvent.of(
                        "USER_PAYMENT_BURST",
                        totalAmount >= RiskRules.BURST_AMOUNT_THRESHOLD ? "CRITICAL" : "WARN",
                        userId,
                        reason,
                        context.window().getStart(),
                        context.window().getEnd(),
                        context.currentWatermark(),
                        Math.max(totalAmount, maxScore),
                        sampleEventId));
            }
        }
    }

    static class TransactionStats implements Serializable {
        private long count;
        private double totalAmount;
        private double totalFraudScore;
        private String sampleEventId;

        void add(TransactionEvent event) {
            count++;
            totalAmount += event.getAmount();
            totalFraudScore += RiskRules.effectiveFraudScore(event);
            if (sampleEventId == null) {
                sampleEventId = event.getEventId();
            }
        }

        TransactionStats merge(TransactionStats other) {
            count += other.count;
            totalAmount += other.totalAmount;
            totalFraudScore += other.totalFraudScore;
            if (sampleEventId == null) {
                sampleEventId = other.sampleEventId;
            }
            return this;
        }
    }

    static class TransactionStatsAggregate implements AggregateFunction<TransactionEvent, TransactionStats, TransactionStats> {
        @Override
        public TransactionStats createAccumulator() {
            return new TransactionStats();
        }

        @Override
        public TransactionStats add(TransactionEvent value, TransactionStats accumulator) {
            accumulator.add(value);
            return accumulator;
        }

        @Override
        public TransactionStats getResult(TransactionStats accumulator) {
            return accumulator;
        }

        @Override
        public TransactionStats merge(TransactionStats a, TransactionStats b) {
            return a.merge(b);
        }
    }

    static class TransactionAggregateWindowFunction
            extends ProcessWindowFunction<TransactionStats, AggregateEvent, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<TransactionStats> stats, Collector<AggregateEvent> out) {
            TransactionStats stat = stats.iterator().next();
            AggregateEvent aggregate = new AggregateEvent();
            aggregate.setAggregateType("COUNTRY_CATEGORY_1M");
            aggregate.setKey(key);
            aggregate.setWindowStart(context.window().getStart());
            aggregate.setWindowEnd(context.window().getEnd());
            aggregate.setEventCount(stat.count);
            aggregate.setTotalAmount(round(stat.totalAmount));
            aggregate.setAvgAmount(stat.count == 0 ? 0.0 : round(stat.totalAmount / stat.count));
            aggregate.setAvgFraudScore(stat.count == 0 ? 0.0 : round(stat.totalFraudScore / stat.count));
            out.collect(aggregate);
        }
    }

    static class MerchantAnomalyWindowFunction
            extends ProcessWindowFunction<TransactionStats, AlertEvent, String, TimeWindow> {
        @Override
        public void process(String merchantId, Context context, Iterable<TransactionStats> stats, Collector<AlertEvent> out) {
            TransactionStats stat = stats.iterator().next();
            double avgFraudScore = stat.count == 0 ? 0.0 : stat.totalFraudScore / stat.count;
            if (!RiskRules.isMerchantAnomaly(stat.count, stat.totalAmount, avgFraudScore)) {
                return;
            }

            String reason = "merchant window anomaly; count="
                    + stat.count
                    + ", totalAmount="
                    + String.format("%.2f", stat.totalAmount)
                    + ", avgFraudScore="
                    + String.format("%.4f", avgFraudScore);

            out.collect(AlertEvent.of(
                    "MERCHANT_ANOMALY",
                    avgFraudScore >= RiskRules.MERCHANT_AVG_FRAUD_SCORE_THRESHOLD ? "CRITICAL" : "WARN",
                    merchantId,
                    reason,
                    context.window().getStart(),
                    context.window().getEnd(),
                    context.currentWatermark(),
                    Math.max(stat.totalAmount, avgFraudScore),
                    stat.sampleEventId));
        }
    }

    static class LateEventDlqMapper implements MapFunction<TransactionEvent, DlqEvent> {
        private final String rawTopic;
        private final String replayTopic;
        private transient ObjectMapper mapper;

        LateEventDlqMapper(String rawTopic, String replayTopic) {
            this.rawTopic = rawTopic;
            this.replayTopic = replayTopic;
        }

        @Override
        public DlqEvent map(TransactionEvent event) {
            if (mapper == null) {
                mapper = ObjectMapperFactory.create();
            }
            return lateEventDlq(event, rawTopic, replayTopic, mapper);
        }
    }

    static class MerchantRiskProfileEnrichmentFunction
            extends BroadcastProcessFunction<TransactionEvent, MerchantRiskProfile, TransactionEvent> {
        private final MapStateDescriptor<String, MerchantRiskProfile> stateDescriptor;

        MerchantRiskProfileEnrichmentFunction(MapStateDescriptor<String, MerchantRiskProfile> stateDescriptor) {
            this.stateDescriptor = stateDescriptor;
        }

        @Override
        public void processElement(
                TransactionEvent event,
                ReadOnlyContext context,
                Collector<TransactionEvent> out) throws Exception {
            ReadOnlyBroadcastState<String, MerchantRiskProfile> state =
                    context.getBroadcastState(stateDescriptor);
            MerchantRiskProfile profile = state.get(normalize(event.getMerchantId(), ""));
            if (profile != null) {
                event.setMerchantRiskTier(profile.getRiskTier());
                event.setMerchantRiskMultiplier(profile.getRiskMultiplier());
                event.setMerchantManualReviewRequired(profile.isManualReviewRequired());
            }
            out.collect(event);
        }

        @Override
        public void processBroadcastElement(
                MerchantRiskProfile profile,
                Context context,
                Collector<TransactionEvent> out) throws Exception {
            BroadcastState<String, MerchantRiskProfile> state = context.getBroadcastState(stateDescriptor);
            if (profile.isDeleted()) {
                state.remove(profile.getMerchantId());
            } else {
                state.put(profile.getMerchantId(), profile);
            }
        }
    }

    private static DlqEvent lateEventDlq(
            TransactionEvent event,
            String rawTopic,
            String replayTopic,
            ObjectMapper mapper) {
        String rawValue;
        try {
            rawValue = mapper.writeValueAsString(event);
        } catch (Exception e) {
            rawValue = event.getEventId();
        }
        return new DlqEvent(
                "LATE_EVENT",
                "event arrived after watermark and allowed lateness",
                rawTopic,
                replayTopic,
                rawValue,
                System.currentTimeMillis());
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
