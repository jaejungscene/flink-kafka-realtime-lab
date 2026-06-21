package com.example.realtimelab.model;

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
}
