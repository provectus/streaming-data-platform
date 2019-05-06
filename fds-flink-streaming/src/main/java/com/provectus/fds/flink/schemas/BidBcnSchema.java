package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class BidBcnSchema implements SerializationSchema<BidBcn>, DeserializationSchema<BidBcn> {
    @Override
    public byte[] serialize(BidBcn bid) {
        try {
            return JsonUtils.write(bid);
        } catch (Exception e) {
            throw new RuntimeException("Error during BidBcn serialization", e);
        }
    }

    @Override
    public BidBcn deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, BidBcn.class);
    }

    @Override
    public boolean isEndOfStream(BidBcn bid) {
        return false;
    }

    @Override
    public TypeInformation<BidBcn> getProducedType() {
        return TypeExtractor.getForClass(BidBcn.class);
    }
}