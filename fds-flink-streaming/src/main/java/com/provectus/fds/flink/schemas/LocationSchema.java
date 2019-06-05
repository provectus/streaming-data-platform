package com.provectus.fds.flink.schemas;

import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.JsonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class LocationSchema implements SerializationSchema<Location>, DeserializationSchema<Location> {
    @Override
    public byte[] serialize(Location location) {
        try {
            return JsonUtils.write(location);
        } catch (Exception e) {
            throw new RuntimeException("Error during Location serialization", e);
        }
    }

    @Override
    public Location deserialize(byte[] bytes) throws IOException {
        return JsonUtils.read(bytes, Location.class);
    }

    @Override
    public boolean isEndOfStream(Location location) {
        return false;
    }

    @Override
    public TypeInformation<Location> getProducedType() {
        return TypeExtractor.getForClass(Location.class);
    }
}