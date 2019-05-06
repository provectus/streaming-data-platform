package com.provectus.fds.flink.config;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class EventTimeExtractor<T> implements AssignerWithPeriodicWatermarks<T> {
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - 5000);
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}