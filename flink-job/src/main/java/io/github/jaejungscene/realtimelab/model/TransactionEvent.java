package io.github.jaejungscene.realtimelab.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;

public class TransactionEvent implements Serializable {
    private String eventId;
    private String userId;
    private String merchantId;
    private String category;
    private long eventTime;
    private double amount;
    private String currency;
    private String country;
    private String channel;
    private String deviceId;
    private double mlFraudScore;
    private String paymentStatus;
    private int ipRisk;
    private String merchantRiskTier;
    private double merchantRiskMultiplier = 1.0;
    private boolean merchantManualReviewRequired;
    private String replayId;
    private String replayRunId;
    private String replaySourceTopic;
    private Integer replaySourcePartition;
    private Long replaySourceOffset;
    private Long replayedFromDlqAt;
    @JsonIgnore
    private String sourceTopic;
    @JsonIgnore
    private int sourcePartition = -1;
    @JsonIgnore
    private long sourceOffset = -1L;
    @JsonIgnore
    private long sourceTimestamp = -1L;
    @JsonIgnore
    private String sourceKey;
    @JsonIgnore
    private String originalRawValue;

    public TransactionEvent() {
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public double getMlFraudScore() {
        return mlFraudScore;
    }

    public void setMlFraudScore(double mlFraudScore) {
        this.mlFraudScore = mlFraudScore;
    }

    public String getPaymentStatus() {
        return paymentStatus;
    }

    public void setPaymentStatus(String paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    public int getIpRisk() {
        return ipRisk;
    }

    public void setIpRisk(int ipRisk) {
        this.ipRisk = ipRisk;
    }

    public String getMerchantRiskTier() {
        return merchantRiskTier;
    }

    public void setMerchantRiskTier(String merchantRiskTier) {
        this.merchantRiskTier = merchantRiskTier;
    }

    public double getMerchantRiskMultiplier() {
        return merchantRiskMultiplier <= 0 ? 1.0 : merchantRiskMultiplier;
    }

    public void setMerchantRiskMultiplier(double merchantRiskMultiplier) {
        this.merchantRiskMultiplier = merchantRiskMultiplier <= 0 ? 1.0 : merchantRiskMultiplier;
    }

    public boolean isMerchantManualReviewRequired() {
        return merchantManualReviewRequired;
    }

    public void setMerchantManualReviewRequired(boolean merchantManualReviewRequired) {
        this.merchantManualReviewRequired = merchantManualReviewRequired;
    }

    public String getReplayId() {
        return replayId;
    }

    public void setReplayId(String replayId) {
        this.replayId = replayId;
    }

    public String getReplayRunId() {
        return replayRunId;
    }

    public void setReplayRunId(String replayRunId) {
        this.replayRunId = replayRunId;
    }

    public String getReplaySourceTopic() {
        return replaySourceTopic;
    }

    public void setReplaySourceTopic(String replaySourceTopic) {
        this.replaySourceTopic = replaySourceTopic;
    }

    public Integer getReplaySourcePartition() {
        return replaySourcePartition;
    }

    public void setReplaySourcePartition(Integer replaySourcePartition) {
        this.replaySourcePartition = replaySourcePartition;
    }

    public Long getReplaySourceOffset() {
        return replaySourceOffset;
    }

    public void setReplaySourceOffset(Long replaySourceOffset) {
        this.replaySourceOffset = replaySourceOffset;
    }

    public Long getReplayedFromDlqAt() {
        return replayedFromDlqAt;
    }

    public void setReplayedFromDlqAt(Long replayedFromDlqAt) {
        this.replayedFromDlqAt = replayedFromDlqAt;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public int getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(int sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public long getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    public long getSourceTimestamp() {
        return sourceTimestamp;
    }

    public void setSourceTimestamp(long sourceTimestamp) {
        this.sourceTimestamp = sourceTimestamp;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public String getOriginalRawValue() {
        return originalRawValue;
    }

    public void setOriginalRawValue(String originalRawValue) {
        this.originalRawValue = originalRawValue;
    }
}
