package io.github.jaejungscene.realtimelab.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jaejungscene.realtimelab.config.JobConfig;
import io.github.jaejungscene.realtimelab.model.AggregateEvent;
import io.github.jaejungscene.realtimelab.model.AlertEvent;
import io.github.jaejungscene.realtimelab.model.DlqEvent;
import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import io.github.jaejungscene.realtimelab.model.MerchantRiskProfile;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import io.github.jaejungscene.realtimelab.rule.RiskRules;
import io.github.jaejungscene.realtimelab.serde.KafkaEnvelopeDeserializationSchema;
import io.github.jaejungscene.realtimelab.serde.ObjectMapperFactory;
import io.github.jaejungscene.realtimelab.sink.KafkaSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Locale;

public class RealTimeAlertJob {
    private static final OutputTag<DlqEvent> DLQ_TAG = new OutputTag<>("dlq") {
    };
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        String rawTopic = config.rawTopic();
        String replayTopic = config.replayTopic();
        String merchantRiskProfileTopic = config.merchantRiskProfileTopic();
        String alertTopic = config.alertTopic();
        String aggregateTopic = config.aggregateTopic();
        String dlqTopic = config.dlqTopic();
        Duration allowedLateness = config.allowedLateness();
        RiskRules riskRules = new RiskRules(config.riskRules());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingMode checkpointingMode =
                config.sinkDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE
                ? CheckpointingMode.EXACTLY_ONCE
                : CheckpointingMode.AT_LEAST_ONCE;
        env.enableCheckpointing(config.checkpointIntervalMillis(), checkpointingMode);

        KafkaSource<KafkaRecord> rawSource = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(List.of(rawTopic, replayTopic))
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(sourceOffsets(config.sourceStartupMode()))
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.sourceIsolationLevel().kafkaValue())
                .setDeserializer(new KafkaEnvelopeDeserializationSchema())
                .build();

        KafkaSource<KafkaRecord> merchantProfileSource = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(merchantRiskProfileTopic)
                .setGroupId(config.consumerGroup() + "-merchant-profiles")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.sourceIsolationLevel().kafkaValue())
                .setDeserializer(new KafkaEnvelopeDeserializationSchema())
                .build();

        SingleOutputStreamOperator<TransactionEvent> parsedEvents = env
                .fromSource(rawSource, WatermarkStrategy.noWatermarks(), "transactions-source")
                .uid(OperatorUids.TRANSACTIONS_SOURCE)
                .process(new TransactionParser(DLQ_TAG, replayTopic, config.maxFutureSkew()))
                .name("parse-transactions")
                .uid(OperatorUids.PARSE_TRANSACTIONS);

        SingleOutputStreamOperator<MerchantRiskProfile> merchantProfiles = env
                .fromSource(merchantProfileSource, WatermarkStrategy.noWatermarks(), "merchant-profile-source")
                .uid(OperatorUids.MERCHANT_PROFILE_SOURCE)
                .process(new MerchantRiskProfileParser(DLQ_TAG))
                .name("parse-merchant-profiles")
                .uid(OperatorUids.PARSE_MERCHANT_PROFILES);

        MapStateDescriptor<String, MerchantRiskProfile> merchantProfileState = merchantProfileStateDescriptor();
        BroadcastStream<MerchantRiskProfile> merchantProfileBroadcast =
                merchantProfiles.broadcast(merchantProfileState);

        SingleOutputStreamOperator<TransactionEvent> enrichedEvents = parsedEvents
                .connect(merchantProfileBroadcast)
                .process(new MerchantRiskProfileEnrichmentFunction(merchantProfileState))
                .name("enrich-with-merchant-risk-profiles")
                .uid(OperatorUids.ENRICH_MERCHANT_RISK);

        SingleOutputStreamOperator<TransactionEvent> eventTimeEvents = enrichedEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TransactionEvent>forBoundedOutOfOrderness(config.watermarkDelay())
                                .withIdleness(config.sourceIdleTimeout())
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime()))
                .name("assign-event-time-watermarks")
                .uid(OperatorUids.ASSIGN_EVENT_TIME);

        SingleOutputStreamOperator<TransactionEvent> events = eventTimeEvents
                .process(new LateEventRouter(WINDOW_SIZE, allowedLateness, rawTopic, replayTopic))
                .name("route-late-events")
                .uid(OperatorUids.ROUTE_LATE_EVENTS);

        parsedEvents
                .getSideOutput(DLQ_TAG)
                .sinkTo(kafkaSink(config, dlqTopic, dlqKey(), "parse-dlq"))
                .name("sink-parse-dlq")
                .uid(OperatorUids.PARSE_DLQ_SINK);

        merchantProfiles
                .getSideOutput(DLQ_TAG)
                .sinkTo(kafkaSink(config, dlqTopic, dlqKey(), "reference-data-dlq"))
                .name("sink-profile-dlq")
                .uid(OperatorUids.PROFILE_DLQ_SINK);

        SingleOutputStreamOperator<AlertEvent> highRiskAlerts = events
                .filter(new HighRiskFilter(riskRules))
                .name("filter-high-risk-transactions")
                .uid(OperatorUids.HIGH_RISK_FILTER)
                .map(new HighRiskAlertMapper(riskRules))
                .name("map-high-risk-alerts")
                .uid(OperatorUids.HIGH_RISK_ALERT_MAPPER);

        highRiskAlerts
                .sinkTo(kafkaSink(config, alertTopic, AlertEvent::getKey, "high-risk-alerts"))
                .name("sink-high-risk-alerts")
                .uid(OperatorUids.HIGH_RISK_ALERT_SINK);

        SingleOutputStreamOperator<AlertEvent> userWindowAlerts = events
                .keyBy(TransactionEvent::getUserId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .process(new UserWindowAlertFunction(riskRules))
                .name("user-window-alerts")
                .uid(OperatorUids.USER_WINDOW_ALERTS);

        userWindowAlerts
                .sinkTo(kafkaSink(config, alertTopic, AlertEvent::getKey, "user-window-alerts"))
                .name("sink-user-window-alerts")
                .uid(OperatorUids.USER_WINDOW_ALERT_SINK);

        SingleOutputStreamOperator<AggregateEvent> aggregates = events
                .keyBy(RealTimeAlertJob::aggregateKey)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .aggregate(new TransactionStatsAggregate(riskRules), new TransactionAggregateWindowFunction())
                .name("country-category-merchant-aggregates")
                .uid(OperatorUids.TRANSACTION_AGGREGATES);

        aggregates
                .sinkTo(kafkaSink(config, aggregateTopic, AggregateEvent::getKey, "transaction-aggregates"))
                .name("sink-transaction-aggregates")
                .uid(OperatorUids.TRANSACTION_AGGREGATE_SINK);

        SingleOutputStreamOperator<AlertEvent> merchantAnomalyAlerts = events
                .keyBy(event -> normalize(event.getMerchantId(), "merchant-unknown"))
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .aggregate(
                        new TransactionStatsAggregate(riskRules),
                        new MerchantAnomalyWindowFunction(riskRules))
                .name("merchant-anomaly-alerts")
                .uid(OperatorUids.MERCHANT_ANOMALY_ALERTS);

        merchantAnomalyAlerts
                .sinkTo(kafkaSink(config, alertTopic, AlertEvent::getKey, "merchant-anomaly-alerts"))
                .name("sink-merchant-anomaly-alerts")
                .uid(OperatorUids.MERCHANT_ANOMALY_ALERT_SINK);

        events
                .getSideOutput(DLQ_TAG)
                .sinkTo(kafkaSink(config, dlqTopic, dlqKey(), "late-events-dlq"))
                .name("sink-late-events-dlq")
                .uid(OperatorUids.LATE_EVENT_DLQ_SINK);

        env.execute("flink-kraft-realtime-lab");
    }

    static String aggregateKey(TransactionEvent event) {
        String country = normalize(event.getCountry(), "UNKNOWN");
        String category = normalize(event.getCategory(), "uncategorized");
        String merchant = normalize(event.getMerchantId(), "merchant-unknown");
        return country + "|" + category + "|" + merchant;
    }

    private static String normalize(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static MapStateDescriptor<String, MerchantRiskProfile> merchantProfileStateDescriptor() {
        return new MapStateDescriptor<>(
                "merchant-risk-profiles",
                String.class,
                MerchantRiskProfile.class);
    }

    private static <T> KafkaSink<T> kafkaSink(
            JobConfig config,
            String topic,
            KafkaSinkFactory.KeyExtractor<T> keyExtractor,
            String transactionalScope) {
        return KafkaSinkFactory.create(
                config.bootstrapServers(),
                topic,
                keyExtractor,
                config.sinkDeliveryGuarantee(),
                config.transactionalIdPrefix(),
                transactionalScope);
    }

    private static KafkaSinkFactory.KeyExtractor<DlqEvent> dlqKey() {
        return event -> normalize(event.getErrorType(), "DLQ");
    }

    private static OffsetsInitializer sourceOffsets(JobConfig.SourceStartupMode startupMode) {
        return startupMode == JobConfig.SourceStartupMode.EARLIEST
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();
    }

    static class LateEventRouter extends ProcessFunction<TransactionEvent, TransactionEvent> {
        private final long windowSizeMillis;
        private final long allowedLatenessMillis;
        private final String rawTopic;
        private final String replayTopic;
        private transient ObjectMapper mapper;

        LateEventRouter(
                Duration windowSize,
                Duration allowedLateness,
                String rawTopic,
                String replayTopic) {
            this.windowSizeMillis = windowSize.toMillis();
            this.allowedLatenessMillis = allowedLateness.toMillis();
            this.rawTopic = rawTopic;
            this.replayTopic = replayTopic;
        }

        @Override
        public void processElement(
                TransactionEvent event,
                Context context,
                Collector<TransactionEvent> out) throws Exception {
            long currentWatermark = context.timerService().currentWatermark();
            if (currentWatermark != Long.MIN_VALUE
                    && isPastWindowCleanup(
                            event.getEventTime(),
                            currentWatermark,
                            windowSizeMillis,
                            allowedLatenessMillis)) {
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
        private final RiskRules riskRules;

        HighRiskFilter(RiskRules riskRules) {
            this.riskRules = riskRules;
        }

        @Override
        public boolean filter(TransactionEvent event) {
            return riskRules.isHighRisk(event);
        }
    }

    static class HighRiskAlertMapper implements MapFunction<TransactionEvent, AlertEvent> {
        private final RiskRules riskRules;

        HighRiskAlertMapper(RiskRules riskRules) {
            this.riskRules = riskRules;
        }

        @Override
        public AlertEvent map(TransactionEvent event) {
            String riskTier = normalize(event.getMerchantRiskTier(), "UNKNOWN");
            double effectiveFraudScore = riskRules.effectiveFraudScore(event);
            return AlertEvent.of(
                    "HIGH_RISK_TRANSACTION",
                    "CRITICAL",
                    event.getUserId(),
                    "single event exceeded fraud rule threshold; merchantRiskTier="
                            + riskTier
                            + ", merchantRiskMultiplier="
                            + String.format(Locale.ROOT, "%.3f", event.getMerchantRiskMultiplier())
                            + ", effectiveFraudScore="
                            + String.format(Locale.ROOT, "%.4f", effectiveFraudScore),
                    event.getEventTime(),
                    event.getEventTime(),
                    event.getEventTime(),
                    "effectiveFraudScore",
                    effectiveFraudScore,
                    event.getEventId());
        }
    }

    static class UserWindowAlertFunction
            extends ProcessWindowFunction<TransactionEvent, AlertEvent, String, TimeWindow> {
        private final RiskRules riskRules;

        UserWindowAlertFunction(RiskRules riskRules) {
            this.riskRules = riskRules;
        }

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<TransactionEvent> events,
                Collector<AlertEvent> out) {
            long count = 0;
            double totalAmount = 0.0;
            String sampleEventId = null;

            for (TransactionEvent event : events) {
                count++;
                totalAmount += event.getAmount();
                if (sampleEventId == null || event.getEventId().compareTo(sampleEventId) < 0) {
                    sampleEventId = event.getEventId();
                }
            }

            if (riskRules.isBurst(count, totalAmount)) {
                boolean amountTriggered = totalAmount >= riskRules.config().burstAmountThreshold();
                String reason = "user window exceeded count or amount threshold; count="
                        + count
                        + ", totalAmount="
                        + String.format(Locale.ROOT, "%.2f", totalAmount);

                out.collect(AlertEvent.of(
                        "USER_PAYMENT_BURST",
                        amountTriggered ? "CRITICAL" : "WARN",
                        userId,
                        reason,
                        context.window().getStart(),
                        context.window().getEnd(),
                        context.window().getEnd(),
                        amountTriggered ? "totalAmount" : "eventCount",
                        amountTriggered ? totalAmount : count,
                        sampleEventId));
            }
        }
    }

    static class TransactionStats implements Serializable {
        private long count;
        private double totalAmount;
        private double totalFraudScore;
        private String sampleEventId;

        void add(TransactionEvent event, RiskRules riskRules) {
            count++;
            totalAmount += event.getAmount();
            totalFraudScore += riskRules.effectiveFraudScore(event);
            if (sampleEventId == null || event.getEventId().compareTo(sampleEventId) < 0) {
                sampleEventId = event.getEventId();
            }
        }

        TransactionStats merge(TransactionStats other) {
            count += other.count;
            totalAmount += other.totalAmount;
            totalFraudScore += other.totalFraudScore;
            if (sampleEventId == null
                    || (other.sampleEventId != null
                            && other.sampleEventId.compareTo(sampleEventId) < 0)) {
                sampleEventId = other.sampleEventId;
            }
            return this;
        }
    }

    static class TransactionStatsAggregate
            implements AggregateFunction<TransactionEvent, TransactionStats, TransactionStats> {
        private final RiskRules riskRules;

        TransactionStatsAggregate(RiskRules riskRules) {
            this.riskRules = riskRules;
        }

        @Override
        public TransactionStats createAccumulator() {
            return new TransactionStats();
        }

        @Override
        public TransactionStats add(TransactionEvent value, TransactionStats accumulator) {
            accumulator.add(value, riskRules);
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
        public void process(
                String key,
                Context context,
                Iterable<TransactionStats> stats,
                Collector<AggregateEvent> out) {
            TransactionStats stat = stats.iterator().next();
            AggregateEvent aggregate = new AggregateEvent();
            aggregate.setAggregateType("COUNTRY_CATEGORY_MERCHANT_1M");
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
        private final RiskRules riskRules;

        MerchantAnomalyWindowFunction(RiskRules riskRules) {
            this.riskRules = riskRules;
        }

        @Override
        public void process(
                String merchantId,
                Context context,
                Iterable<TransactionStats> stats,
                Collector<AlertEvent> out) {
            TransactionStats stat = stats.iterator().next();
            double avgFraudScore = stat.count == 0 ? 0.0 : stat.totalFraudScore / stat.count;
            if (!riskRules.isMerchantAnomaly(stat.count, stat.totalAmount, avgFraudScore)) {
                return;
            }

            String reason = "merchant window anomaly; count="
                    + stat.count
                    + ", totalAmount="
                    + String.format(Locale.ROOT, "%.2f", stat.totalAmount)
                    + ", avgFraudScore="
                    + String.format(Locale.ROOT, "%.4f", avgFraudScore);

            boolean riskTriggered = avgFraudScore
                    >= riskRules.config().merchantAvgFraudScoreThreshold();
            boolean amountTriggered = stat.totalAmount >= riskRules.config().merchantAmountThreshold();
            out.collect(AlertEvent.of(
                    "MERCHANT_ANOMALY",
                    riskTriggered ? "CRITICAL" : "WARN",
                    merchantId,
                    reason,
                    context.window().getStart(),
                    context.window().getEnd(),
                    context.window().getEnd(),
                    riskTriggered ? "avgFraudScore" : amountTriggered ? "totalAmount" : "eventCount",
                    riskTriggered ? avgFraudScore : amountTriggered ? stat.totalAmount : stat.count,
                    stat.sampleEventId));
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
        String rawValue = event.getOriginalRawValue();
        if (rawValue == null || rawValue.isBlank()) {
            try {
                rawValue = mapper.writeValueAsString(event);
            } catch (Exception e) {
                rawValue = event.getEventId();
            }
        }
        return new DlqEvent(
                "LATE_EVENT",
                "event arrived after the window cleanup deadline",
                normalize(event.getSourceTopic(), rawTopic),
                event.getSourcePartition() < 0 ? null : event.getSourcePartition(),
                event.getSourceOffset() < 0 ? null : event.getSourceOffset(),
                event.getSourceTimestamp() < 0 ? null : event.getSourceTimestamp(),
                event.getSourceKey(),
                replayTopic,
                rawValue,
                System.currentTimeMillis());
    }

    static boolean isPastWindowCleanup(
            long eventTime,
            long currentWatermark,
            long windowSizeMillis,
            long allowedLatenessMillis) {
        if (windowSizeMillis <= 0 || allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("window size must be positive and lateness must not be negative");
        }
        long windowStart = Math.floorDiv(eventTime, windowSizeMillis) * windowSizeMillis;
        long windowMaxTimestamp = windowStart + windowSizeMillis - 1;
        return windowMaxTimestamp + allowedLatenessMillis <= currentWatermark;
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
