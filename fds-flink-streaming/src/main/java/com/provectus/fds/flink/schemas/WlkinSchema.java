package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.Wlkin;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class WlkinSchema implements SerializationSchema<Wlkin>, DeserializationSchema<Wlkin> {
    @Override
    public byte[] serialize(Wlkin wlkin) {
        try {
            return JsonUtils.write(wlkin);
        } catch (Exception e) {
            throw new RuntimeException("Error during Wlkin serialization", e);
        }
    }

    @Override
    public Wlkin deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, Wlkin.class);
    }

    @Override
    public boolean isEndOfStream(Wlkin wlkin) {
        return false;
    }

    @Override
    public TypeInformation<Wlkin> getProducedType() {
        return TypeExtractor.getForClass(Wlkin.class);
    }
}