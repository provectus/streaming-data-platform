package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.WlkinClick;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class WlkinClickSchema implements SerializationSchema<WlkinClick>, DeserializationSchema<WlkinClick> {
    @Override
    public byte[] serialize(WlkinClick wlkinClick) {
        try {
            return JsonUtils.write(wlkinClick);
        } catch (Exception e) {
            throw new RuntimeException("Error during WlkinClick serialization", e);
        }
    }

    @Override
    public WlkinClick deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, WlkinClick.class);
    }

    @Override
    public boolean isEndOfStream(WlkinClick wlkinClick) {
        return false;
    }

    @Override
    public TypeInformation<WlkinClick> getProducedType() {
        return TypeExtractor.getForClass(WlkinClick.class);
    }
}