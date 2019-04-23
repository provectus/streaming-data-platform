package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.ClickBcn;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class ClickBcnSchema implements SerializationSchema<ClickBcn>, DeserializationSchema<ClickBcn> {
    @Override
    public byte[] serialize(ClickBcn click) {
        try {
            return JsonUtils.write(click);
        } catch (Exception e) {
            throw new RuntimeException("Error during ClickBcn serialization", e);
        }
    }

    @Override
    public ClickBcn deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, ClickBcn.class);
    }

    @Override
    public boolean isEndOfStream(ClickBcn click) {
        return false;
    }

    @Override
    public TypeInformation<ClickBcn> getProducedType() {
        return TypeExtractor.getForClass(ClickBcn.class);
    }
}