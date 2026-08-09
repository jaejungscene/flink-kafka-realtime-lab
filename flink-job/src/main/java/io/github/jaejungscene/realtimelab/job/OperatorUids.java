package io.github.jaejungscene.realtimelab.job;

import java.util.List;

final class OperatorUids {
    static final String TRANSACTIONS_SOURCE = "transactions-source-v1";
    static final String PARSE_TRANSACTIONS = "parse-transactions-v1";
    static final String DEDUPLICATE_EVENTS = "deduplicate-events-v1";
    static final String MERCHANT_PROFILE_SOURCE = "merchant-profile-source-v1";
    static final String PARSE_MERCHANT_PROFILES = "parse-merchant-profiles-v1";
    static final String ENRICH_MERCHANT_RISK = "enrich-merchant-risk-v1";
    static final String ASSIGN_EVENT_TIME = "assign-event-time-v1";
    static final String ROUTE_LATE_EVENTS = "route-late-events-v1";
    static final String PARSE_DLQ_SINK = "parse-dlq-sink-v1";
    static final String PROFILE_DLQ_SINK = "profile-dlq-sink-v1";
    static final String HIGH_RISK_FILTER = "high-risk-filter-v1";
    static final String HIGH_RISK_ALERT_MAPPER = "high-risk-alert-mapper-v1";
    static final String HIGH_RISK_ALERT_SINK = "high-risk-alert-sink-v1";
    static final String USER_WINDOW_ALERTS = "user-window-alerts-v2";
    static final String USER_WINDOW_ALERT_SINK = "user-window-alert-sink-v1";
    static final String TRANSACTION_AGGREGATES = "transaction-aggregates-v1";
    static final String TRANSACTION_AGGREGATE_SINK = "transaction-aggregate-sink-v1";
    static final String MERCHANT_ANOMALY_ALERTS = "merchant-anomaly-alerts-v1";
    static final String MERCHANT_ANOMALY_ALERT_SINK = "merchant-anomaly-alert-sink-v1";
    static final String LATE_EVENT_DLQ_SINK = "late-event-dlq-sink-v1";

    private OperatorUids() {
    }

    static List<String> all() {
        return List.of(
                TRANSACTIONS_SOURCE,
                PARSE_TRANSACTIONS,
                DEDUPLICATE_EVENTS,
                MERCHANT_PROFILE_SOURCE,
                PARSE_MERCHANT_PROFILES,
                ENRICH_MERCHANT_RISK,
                ASSIGN_EVENT_TIME,
                ROUTE_LATE_EVENTS,
                PARSE_DLQ_SINK,
                PROFILE_DLQ_SINK,
                HIGH_RISK_FILTER,
                HIGH_RISK_ALERT_MAPPER,
                HIGH_RISK_ALERT_SINK,
                USER_WINDOW_ALERTS,
                USER_WINDOW_ALERT_SINK,
                TRANSACTION_AGGREGATES,
                TRANSACTION_AGGREGATE_SINK,
                MERCHANT_ANOMALY_ALERTS,
                MERCHANT_ANOMALY_ALERT_SINK,
                LATE_EVENT_DLQ_SINK);
    }
}
