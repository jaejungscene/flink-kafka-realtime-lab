package com.example.realtimelab.model;

import java.io.Serializable;

public class MerchantRiskProfile implements Serializable {
    private String merchantId;
    private String riskTier;
    private double riskMultiplier;
    private boolean manualReviewRequired;
    private String updatedAt;
    private boolean deleted;

    public MerchantRiskProfile() {
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getRiskTier() {
        return riskTier;
    }

    public void setRiskTier(String riskTier) {
        this.riskTier = riskTier;
    }

    public double getRiskMultiplier() {
        return riskMultiplier;
    }

    public void setRiskMultiplier(double riskMultiplier) {
        this.riskMultiplier = riskMultiplier;
    }

    public boolean isManualReviewRequired() {
        return manualReviewRequired;
    }

    public void setManualReviewRequired(boolean manualReviewRequired) {
        this.manualReviewRequired = manualReviewRequired;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
