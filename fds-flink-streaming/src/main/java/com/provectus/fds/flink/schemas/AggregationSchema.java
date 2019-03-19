package com.provectus.fds.flink.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class AggregationSchema implements SerializationSchema<Aggregation> {
    @Override
    public byte[] serialize(Aggregation aggregation) {
        try {
            return JsonUtils.write(aggregation);
        } catch (Exception e) {
            throw new RuntimeException("Error during Aggregation serialization", e);
        }
    }
}