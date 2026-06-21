package com.example.realtimelab.model;

import java.io.Serializable;

public class DlqEvent implements Serializable {
    private String errorType;
    private String reason;
    private String sourceTopic;
    private String replayTopic;
    private String rawValue;
    private long observedAt;

    public DlqEvent() {
    }

    public DlqEvent(String errorType, String reason, String rawValue, long observedAt) {
        this(errorType, reason, null, null, rawValue, observedAt);
    }

    public DlqEvent(String errorType, String reason, String sourceTopic, String replayTopic, String rawValue, long observedAt) {
        this.errorType = errorType;
        this.reason = reason;
        this.sourceTopic = sourceTopic;
        this.replayTopic = replayTopic;
        this.rawValue = rawValue;
        this.observedAt = observedAt;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getReplayTopic() {
        return replayTopic;
    }

    public void setReplayTopic(String replayTopic) {
        this.replayTopic = replayTopic;
    }

    public String getRawValue() {
        return rawValue;
    }

    public void setRawValue(String rawValue) {
        this.rawValue = rawValue;
    }

    public long getObservedAt() {
        return observedAt;
    }

    public void setObservedAt(long observedAt) {
        this.observedAt = observedAt;
    }
}
