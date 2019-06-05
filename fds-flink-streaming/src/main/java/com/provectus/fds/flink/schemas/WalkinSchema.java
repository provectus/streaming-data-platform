package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.Walkin;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class WalkinSchema implements SerializationSchema<Walkin>, DeserializationSchema<Walkin> {
    @Override
    public byte[] serialize(Walkin walkin) {
        try {
            return JsonUtils.write(walkin);
        } catch (Exception e) {
            throw new RuntimeException("Error during Walkin serialization", e);
        }
    }

    @Override
    public Walkin deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, Walkin.class);
    }

    @Override
    public boolean isEndOfStream(Walkin walkin) {
        return false;
    }

    @Override
    public TypeInformation<Walkin> getProducedType() {
        return TypeExtractor.getForClass(Walkin.class);
    }
}