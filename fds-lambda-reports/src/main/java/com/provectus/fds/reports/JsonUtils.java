package com.provectus.fds.reports;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;

public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static JsonNode readTree(Reader reader) throws IOException {
       return objectMapper.readTree(reader);
    }

    public static <T> byte[] write(T value) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(value);
    }

    public static <T> T read(Reader reader, Class clazz) throws IOException {
        return objectMapper.readerFor(clazz).readValue(reader);
    }


    public static  <T> String stringify(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }
}
