package io.github.jaejungscene.realtimelab.job;

import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

final class EventDeduplicator
        extends KeyedProcessFunction<String, TransactionEvent, TransactionEvent> {
    private final Duration stateTtl;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicateEvents;

    EventDeduplicator(Duration stateTtl) {
        if (stateTtl == null || stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be greater than zero");
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(stateTtl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        seen = getRuntimeContext().getState(descriptor);
        duplicateEvents = getRuntimeContext()
                .getMetricGroup()
                .counter("duplicate_events_total");
    }

    @Override
    public void processElement(
            TransactionEvent event,
            Context context,
            Collector<TransactionEvent> out) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            duplicateEvents.inc();
            return;
        }
        seen.update(Boolean.TRUE);
        out.collect(event);
    }
}
