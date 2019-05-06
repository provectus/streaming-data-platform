package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class BcnSchema implements SerializationSchema<Bcn>, DeserializationSchema<Bcn> {
    @Override
    public byte[] serialize(Bcn bcn) {
        try {
            return JsonUtils.write(bcn);
        } catch (Exception e) {
            throw new RuntimeException("Error during Bcn serialization", e);
        }
    }

    @Override
    public Bcn deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, Bcn.class);
    }

    @Override
    public boolean isEndOfStream(Bcn bcn) {
        return false;
    }

    @Override
    public TypeInformation<Bcn> getProducedType() {
        return TypeExtractor.getForClass(Bcn.class);
    }
}