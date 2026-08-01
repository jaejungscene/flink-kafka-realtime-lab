package io.github.jaejungscene.realtimelab.model;

import java.io.Serializable;

public class DlqEvent implements Serializable {
    private int schemaVersion = 1;
    private String errorType;
    private String reason;
    private String sourceTopic;
    private Integer sourcePartition;
    private Long sourceOffset;
    private Long sourceTimestamp;
    private String sourceKey;
    private String replayTopic;
    private String rawValue;
    private long observedAt;

    public DlqEvent() {
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public DlqEvent(String errorType, String reason, String rawValue, long observedAt) {
        this(errorType, reason, null, null, rawValue, observedAt);
    }

    public DlqEvent(
            String errorType,
            String reason,
            String sourceTopic,
            String replayTopic,
            String rawValue,
            long observedAt) {
        this(errorType, reason, sourceTopic, null, null, null, null, replayTopic, rawValue, observedAt);
    }

    public DlqEvent(
            String errorType,
            String reason,
            String sourceTopic,
            Integer sourcePartition,
            Long sourceOffset,
            Long sourceTimestamp,
            String sourceKey,
            String replayTopic,
            String rawValue,
            long observedAt) {
        this.errorType = errorType;
        this.reason = reason;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.sourceTimestamp = sourceTimestamp;
        this.sourceKey = sourceKey;
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

    public Integer getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(Integer sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public Long getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(Long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    public Long getSourceTimestamp() {
        return sourceTimestamp;
    }

    public void setSourceTimestamp(Long sourceTimestamp) {
        this.sourceTimestamp = sourceTimestamp;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
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
