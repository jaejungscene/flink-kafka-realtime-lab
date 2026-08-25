package io.github.jaejungscene.realtimelab.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jaejungscene.realtimelab.model.DlqEvent;
import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import io.github.jaejungscene.realtimelab.serde.ObjectMapperFactory;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TransactionParser extends ProcessFunction<KafkaRecord, TransactionEvent> {
    static final String RISK_CURRENCY = "USD";
    private final OutputTag<DlqEvent> dlqTag;
    private final String replayTopic;
    private final long maxFutureSkewMillis;
    private transient ObjectMapper mapper;

    public TransactionParser(OutputTag<DlqEvent> dlqTag) {
        this(dlqTag, null, Duration.ofMinutes(5));
    }

    public TransactionParser(OutputTag<DlqEvent> dlqTag, String replayTopic, Duration maxFutureSkew) {
        this.dlqTag = dlqTag;
        this.replayTopic = replayTopic;
        this.maxFutureSkewMillis = maxFutureSkew.toMillis();
    }

    @Override
    public void processElement(KafkaRecord record, Context ctx, Collector<TransactionEvent> out) {
        try {
            if (mapper == null) {
                mapper = ObjectMapperFactory.create();
            }
            out.collect(parse(record, mapper, System.currentTimeMillis(), maxFutureSkewMillis));
        } catch (Exception e) {
            ctx.output(dlqTag, new DlqEvent(
                    "PARSE_OR_VALIDATION_ERROR",
                    errorMessage(e),
                    record.getTopic(),
                    record.getPartition(),
                    record.getOffset(),
                    record.getTimestamp(),
                    record.getKey(),
                    replayTopic,
                    record.getValue(),
                    System.currentTimeMillis()));
        }
    }

    static TransactionEvent parse(KafkaRecord record, ObjectMapper mapper) throws Exception {
        return parse(record, mapper, System.currentTimeMillis(), Duration.ofMinutes(5).toMillis());
    }

    static TransactionEvent parse(
            KafkaRecord record,
            ObjectMapper mapper,
            long currentTimeMillis,
            long maxFutureSkewMillis) throws Exception {
        if (record.getValue() == null || record.getValue().isBlank()) {
            throw new IllegalArgumentException("record value must not be null or blank");
        }
        TransactionEvent event = mapper.readValue(record.getValue(), TransactionEvent.class);
        validate(event, currentTimeMillis, maxFutureSkewMillis);
        event.setSourceTopic(record.getTopic());
        event.setSourcePartition(record.getPartition());
        event.setSourceOffset(record.getOffset());
        event.setSourceTimestamp(record.getTimestamp());
        event.setSourceKey(record.getKey());
        event.setOriginalRawValue(record.getValue());
        return event;
    }

    static void validate(TransactionEvent event, long currentTimeMillis, long maxFutureSkewMillis) {
        if (maxFutureSkewMillis < 0) {
            throw new IllegalArgumentException("maxFutureSkewMillis must not be negative");
        }
        event.setEventId(requiredIdentifier(event.getEventId(), "eventId"));
        event.setUserId(requiredIdentifier(event.getUserId(), "userId"));
        if (event.getMerchantId() != null) {
            event.setMerchantId(optionalIdentifier(event.getMerchantId(), "merchantId"));
        }
        if (event.getSchemaVersion() < 1) {
            throw new IllegalArgumentException("schemaVersion must be greater than 0");
        }
        if (event.getEventTime() <= 0) {
            throw new IllegalArgumentException("eventTime must be epoch millis");
        }
        if (event.getEventTime() > currentTimeMillis + maxFutureSkewMillis) {
            throw new IllegalArgumentException("eventTime exceeds the configured future-skew limit");
        }
        if (!Double.isFinite(event.getAmount()) || event.getAmount() < 0) {
            throw new IllegalArgumentException("amount must be a finite non-negative number");
        }
        String currency = event.getCurrency();
        if (currency == null || currency.isBlank()) {
            event.setCurrency(RISK_CURRENCY);
        } else if (!RISK_CURRENCY.equalsIgnoreCase(currency.trim())) {
            throw new IllegalArgumentException(
                    "currency must be " + RISK_CURRENCY + " for configured amount thresholds");
        } else {
            event.setCurrency(RISK_CURRENCY);
        }
        if (!Double.isFinite(event.getMlFraudScore())
                || event.getMlFraudScore() < 0
                || event.getMlFraudScore() > 1) {
            throw new IllegalArgumentException("mlFraudScore must be between 0 and 1");
        }
        if (event.getIpRisk() < 0 || event.getIpRisk() > 100) {
            throw new IllegalArgumentException("ipRisk must be between 0 and 100");
        }
        if (event.getPaymentStatus() != null) {
            String paymentStatus = event.getPaymentStatus().trim();
            event.setPaymentStatus(paymentStatus.isEmpty() ? null : paymentStatus);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String requiredIdentifier(String value, String fieldName) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        return boundedIdentifier(value, fieldName);
    }

    private static String optionalIdentifier(String value, String fieldName) {
        if (value.isBlank()) {
            return null;
        }
        return boundedIdentifier(value, fieldName);
    }

    private static String boundedIdentifier(String value, String fieldName) {
        String normalized = value.trim();
        if (normalized.length() > 256) {
            throw new IllegalArgumentException(fieldName + " must not exceed 256 characters");
        }
        return normalized;
    }

    private static String errorMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }
}
