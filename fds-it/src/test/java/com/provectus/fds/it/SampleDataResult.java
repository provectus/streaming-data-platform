package com.provectus.fds.it;

public class SampleDataResult {

    long campaignItemId;
    int countOfBids;
    int countOfImpressions;
    int countOfClicks;

    public SampleDataResult(long campaignItemId) {
        this.campaignItemId = campaignItemId;
    }

    public long getCampaignItemId() {
        return campaignItemId;
    }

    public int getCountOfBids() {
        return countOfBids;
    }

    public int getCountOfImpressions() {
        return countOfImpressions;
    }

    public int getCountOfClicks() {
        return countOfClicks;
    }

    public void setCountOfBids(int countOfBids) {
        this.countOfBids = countOfBids;
    }

    public void setCountOfImpressions(int countOfImpressions) {
        this.countOfImpressions = countOfImpressions;
    }

    public void setCountOfClicks(int countOfClicks) {
        this.countOfClicks = countOfClicks;
    }
}
