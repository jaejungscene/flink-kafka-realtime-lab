package com.example.realtimelab.model;

import java.io.Serializable;

public class AggregateEvent implements Serializable {
    private String aggregateType;
    private String key;
    private long windowStart;
    private long windowEnd;
    private long eventCount;
    private double totalAmount;
    private double avgAmount;
    private double avgFraudScore;

    public AggregateEvent() {
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getEventCount() {
        return eventCount;
    }

    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public double getAvgAmount() {
        return avgAmount;
    }

    public void setAvgAmount(double avgAmount) {
        this.avgAmount = avgAmount;
    }

    public double getAvgFraudScore() {
        return avgFraudScore;
    }

    public void setAvgFraudScore(double avgFraudScore) {
        this.avgFraudScore = avgFraudScore;
    }
}
