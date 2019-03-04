package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.bcns.ImpressionBcn;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class ImpressionSchema implements DeserializationSchema<ImpressionBcn> {
    @Override
    public ImpressionBcn deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, ImpressionBcn.class);
    }

    @Override
    public boolean isEndOfStream(ImpressionBcn impressionBcn) {
        return false;
    }

    @Override
    public TypeInformation<ImpressionBcn> getProducedType() {
        return TypeExtractor.getForClass(ImpressionBcn.class);
    }
}