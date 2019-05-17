package com.provectus.fds.ml;

import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.io.IOException;

public class PredictRequest {
    private String campaignItemId;
    private String domain;
    private String creativeId;
    private String creativeCategory;
    private Double winPrice;

    public PredictRequest() {
    }

    public PredictRequest(String campaignItemId, String domain, String creativeId, String creativeCategory, Double winPrice) {
        this.campaignItemId = campaignItemId;
        this.domain = domain;
        this.creativeId = creativeId;
        this.creativeCategory = creativeCategory;
        this.winPrice = winPrice;
    }

    public String getCampaignItemId() {
        return campaignItemId;
    }

    public Double getCategorizedCampaignItemId() throws IOException {
        return calculateCategory(campaignItemId);
    }

    public void setCampaignItemId(String campaignItemId) {
        this.campaignItemId = campaignItemId;
    }

    public String getDomain() {
        return domain;
    }

    public Double getCategorizedDomain() throws IOException {
        return calculateCategory(domain);
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getCreativeId() {
        return creativeId;
    }

    public Double getCategorizedCreativeId() throws IOException {
        return calculateCategory(creativeId);
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }

    public String getCreativeCategory() {
        return creativeCategory;
    }

    public Double getCategorizedCreativeCategory() throws IOException {
        return calculateCategory(creativeCategory);
    }

    public void setCreativeCategory(String creativeCategory) {
        this.creativeCategory = creativeCategory;
    }

    public Double getWinPrice() {
        return winPrice;
    }

    public void setWinPrice(Double winPrice) {
        this.winPrice = winPrice;
    }

    @Override
    public String toString() {
        return "PredictRequest{" +
                "campaignItemId='" + campaignItemId + '\'' +
                ", domain='" + domain + '\'' +
                ", creativeId='" + creativeId + '\'' +
                ", creativeCategory='" + creativeCategory + '\'' +
                ", winPrice=" + winPrice +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PredictRequest request = (PredictRequest) o;

        if (campaignItemId != null ? !campaignItemId.equals(request.campaignItemId) : request.campaignItemId != null)
            return false;
        if (domain != null ? !domain.equals(request.domain) : request.domain != null) return false;
        if (creativeId != null ? !creativeId.equals(request.creativeId) : request.creativeId != null) return false;
        if (creativeCategory != null ? !creativeCategory.equals(request.creativeCategory) : request.creativeCategory != null)
            return false;
        return winPrice != null ? winPrice.equals(request.winPrice) : request.winPrice == null;

    }

    @Override
    public int hashCode() {
        int result = campaignItemId != null ? campaignItemId.hashCode() : 0;
        result = 31 * result + (domain != null ? domain.hashCode() : 0);
        result = 31 * result + (creativeId != null ? creativeId.hashCode() : 0);
        result = 31 * result + (creativeCategory != null ? creativeCategory.hashCode() : 0);
        result = 31 * result + (winPrice != null ? winPrice.hashCode() : 0);
        return result;
    }

    private double calculateCategory(String string) throws IOException {
        return (calculateXXHash64(string) & 9223372036854775807L) / 9223372036854775807D;
    }

    private long calculateXXHash64(String arg) throws IOException {
        return XxHash64.hash(Slices.utf8Slice(arg));
    }
}
