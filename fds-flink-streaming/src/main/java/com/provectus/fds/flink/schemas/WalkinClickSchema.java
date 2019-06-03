package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.WalkinClick;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class WalkinClickSchema implements SerializationSchema<WalkinClick>, DeserializationSchema<WalkinClick> {
    @Override
    public byte[] serialize(WalkinClick walkinClick) {
        try {
            return JsonUtils.write(walkinClick);
        } catch (Exception e) {
            throw new RuntimeException("Error during WalkinClick serialization", e);
        }
    }

    @Override
    public WalkinClick deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, WalkinClick.class);
    }

    @Override
    public boolean isEndOfStream(WalkinClick walkinClick) {
        return false;
    }

    @Override
    public TypeInformation<WalkinClick> getProducedType() {
        return TypeExtractor.getForClass(WalkinClick.class);
    }
}