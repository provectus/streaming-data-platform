package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class AggregationSchema implements SerializationSchema<Aggregation>, DeserializationSchema<Aggregation> {
    @Override
    public byte[] serialize(Aggregation aggregation) {
        try {
            return JsonUtils.write(aggregation);
        } catch (Exception e) {
            throw new RuntimeException("Error during Aggregation serialization", e);
        }
    }

    @Override
    public Aggregation deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, Aggregation.class);
    }

    @Override
    public boolean isEndOfStream(Aggregation aggregation) {
        return false;
    }

    @Override
    public TypeInformation<Aggregation> getProducedType() {
        return TypeExtractor.getForClass(Aggregation.class);
    }
}