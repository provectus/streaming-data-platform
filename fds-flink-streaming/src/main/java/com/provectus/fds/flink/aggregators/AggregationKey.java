package com.provectus.fds.flink.aggregators;

import com.provectus.fds.models.events.Aggregation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregationKey {
    private long campaignItemId;
    private String period;

    public static AggregationKey of(Aggregation aggregation) {
        return new AggregationKey(aggregation.getCampaignItemId(), aggregation.getPeriod());
    }
}
