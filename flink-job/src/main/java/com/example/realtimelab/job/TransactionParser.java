package com.example.realtimelab.job;

import com.example.realtimelab.model.DlqEvent;
import com.example.realtimelab.model.TransactionEvent;
import com.example.realtimelab.serde.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class TransactionParser extends ProcessFunction<String, TransactionEvent> {
    private final OutputTag<DlqEvent> dlqTag;
    private final String sourceTopics;
    private final String replayTopic;
    private transient ObjectMapper mapper;

    public TransactionParser(OutputTag<DlqEvent> dlqTag) {
        this(dlqTag, null, null);
    }

    public TransactionParser(OutputTag<DlqEvent> dlqTag, String sourceTopics, String replayTopic) {
        this.dlqTag = dlqTag;
        this.sourceTopics = sourceTopics;
        this.replayTopic = replayTopic;
    }

    @Override
    public void processElement(String rawValue, Context ctx, Collector<TransactionEvent> out) {
        try {
            if (mapper == null) {
                mapper = ObjectMapperFactory.create();
            }
            TransactionEvent event = mapper.readValue(rawValue, TransactionEvent.class);
            validate(event);
            out.collect(event);
        } catch (Exception e) {
            ctx.output(dlqTag, new DlqEvent(
                    "PARSE_OR_VALIDATION_ERROR",
                    e.getMessage(),
                    sourceTopics,
                    replayTopic,
                    rawValue,
                    System.currentTimeMillis()));
        }
    }

    private static void validate(TransactionEvent event) {
        if (isBlank(event.getEventId())) {
            throw new IllegalArgumentException("eventId is required");
        }
        if (isBlank(event.getUserId())) {
            throw new IllegalArgumentException("userId is required");
        }
        if (event.getEventTime() <= 0) {
            throw new IllegalArgumentException("eventTime must be epoch millis");
        }
        if (event.getAmount() < 0) {
            throw new IllegalArgumentException("amount must be non-negative");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
