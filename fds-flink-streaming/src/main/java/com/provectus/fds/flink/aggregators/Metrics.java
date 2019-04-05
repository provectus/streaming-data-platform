package com.provectus.fds.flink.aggregators;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Metrics {
    private long clicks;
    private long impressions;
    private long bids;
}