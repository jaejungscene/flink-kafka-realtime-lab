package com.example.realtimelab.model;

import java.io.Serializable;
import java.util.UUID;

public class AlertEvent implements Serializable {
    private String alertId;
    private String alertType;
    private String severity;
    private String key;
    private String reason;
    private long windowStart;
    private long windowEnd;
    private long eventTime;
    private double metricValue;
    private String sampleEventId;

    public AlertEvent() {
    }

    public static AlertEvent of(
            String alertType,
            String severity,
            String key,
            String reason,
            long windowStart,
            long windowEnd,
            long eventTime,
            double metricValue,
            String sampleEventId) {
        AlertEvent alert = new AlertEvent();
        alert.setAlertId(UUID.randomUUID().toString());
        alert.setAlertType(alertType);
        alert.setSeverity(severity);
        alert.setKey(key);
        alert.setReason(reason);
        alert.setWindowStart(windowStart);
        alert.setWindowEnd(windowEnd);
        alert.setEventTime(eventTime);
        alert.setMetricValue(metricValue);
        alert.setSampleEventId(sampleEventId);
        return alert;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
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

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public double getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(double metricValue) {
        this.metricValue = metricValue;
    }

    public String getSampleEventId() {
        return sampleEventId;
    }

    public void setSampleEventId(String sampleEventId) {
        this.sampleEventId = sampleEventId;
    }
}
