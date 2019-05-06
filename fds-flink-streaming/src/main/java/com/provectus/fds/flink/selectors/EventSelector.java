package com.provectus.fds.flink.selectors;

import com.provectus.fds.models.bcns.Bcn;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class EventSelector implements OutputSelector<Bcn> {
    private static final long serialVersionUID = -8133060755314810477L;

    @Override
    public Iterable<String> select(Bcn event) {
        return java.util.Collections.singletonList(event.getType());
    }
}